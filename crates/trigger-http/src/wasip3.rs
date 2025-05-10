use anyhow::{anyhow, Context, Result};
use futures::{channel::mpsc, FutureExt, SinkExt};
use http::{HeaderName, HeaderValue};
use http_body_util::{combinators::BoxBody, BodyExt, StreamBody};
use hyper::{
    body::{Bytes, Frame},
    Request, Response,
};
use spin_factor_outbound_http::OutboundHttpFactor;
use spin_factors::RuntimeFactors;
use spin_factors_executor::InstanceState;
use spin_http::routes::RouteMatch;
use std::future::Future;
use std::io::Cursor;
use std::net::SocketAddr;
use tracing::{instrument, Level};
use wasi_http_draft::{wasi::http::types, WasiHttpView};
use wasmtime::component::{
    self, ErrorContext, FutureReader, FutureWriter, Resource, StreamReader, StreamWriter,
};
use wasmtime_wasi_http::{
    bindings::http::types::ErrorCode,
    body::{HyperIncomingBody as Body, HyperOutgoingBody},
};

use crate::{headers::prepare_request_headers, server::HttpExecutor, TriggerInstanceBuilder};

type Store<T> = spin_core::Store<InstanceState<T, ()>>;

type PromisesUnordered<T> = futures::stream::FuturesUnordered<Box<dyn Future<Output = Result<T>>>>;

mod proxy {
    wasmtime::component::bindgen!({
        path: "../../http/wit",
        world: "wasi:http/proxy",
        concurrent_imports: true,
        concurrent_exports: true,
        async: {
            only_imports: [
                "wasi:http/types@0.3.0-draft#[static]body.finish",
                "wasi:http/handler@0.3.0-draft#handle",
            ]
        },
        with: {
            "wasi:http/types": wasi_http_draft::wasi::http::types,
        }
    });
}

enum Event {
    RequestBodyWrite(
        Body,
        Option<StreamWriter<Bytes>>,
        FutureWriter<Resource<types::Fields>>,
    ),
    RequestTrailersWrite,
    Response(Result<Resource<types::Response>, types::ErrorCode>),
    ResponseBodyRead(
        Result<(StreamReader<Bytes>, Bytes), Option<ErrorContext>>,
        Option<FutureReader<Resource<types::Fields>>>,
    ),
    ResponseTrailersRead(Result<Resource<types::Fields>, Option<ErrorContext>>),
}

/// An [`HttpExecutor`] that uses the `wasi:http@0.3.0-draft/handler` interface.
#[derive(Clone)]
pub struct Wasip3HttpExecutor;

impl HttpExecutor for Wasip3HttpExecutor {
    #[instrument(name = "spin_trigger_http.execute_wasm", skip_all, err(level = Level::INFO), fields(otel.name = format!("execute_wasm_component {}", route_match.component_id())))]
    async fn execute<F: RuntimeFactors>(
        &self,
        instance_builder: TriggerInstanceBuilder<'_, F>,
        route_match: &RouteMatch<'_, '_>,
        mut request: Request<Body>,
        client_addr: SocketAddr,
    ) -> Result<Response<Body>> {
        let component_id = route_match.component_id();

        tracing::trace!("Executing request using the Wasip3 executor for component {component_id}");

        let (instance, mut store) = instance_builder.instantiate(()).await?;

        let headers = prepare_request_headers(&request, route_match, client_addr)?;
        request.headers_mut().clear();
        request
            .headers_mut()
            .extend(headers.into_iter().filter_map(|(n, v)| {
                let Ok(name) = n.parse::<HeaderName>() else {
                    return None;
                };
                let Ok(value) = HeaderValue::from_bytes(v.as_bytes()) else {
                    return None;
                };
                Some((name, value))
            }));

        let (request_body_tx, request_body_rx) = instance.stream(&mut store)?;

        let (request_trailers_tx, request_trailers_rx) = instance.future(&mut store)?;

        let mut wasi_http =
            spin_factor_outbound_http::OutboundHttpFactor::get_wasi_http_draft_impl(
                store.data_mut().factors_instance_state_mut(),
            )
            .context("missing OutboundHttpFactor")?;

        let my_request = wasi_http.table().push(types::Request {
            method: match request.method() {
                &hyper::Method::GET => types::Method::Get,
                &hyper::Method::POST => types::Method::Post,
                &hyper::Method::PUT => types::Method::Put,
                &hyper::Method::DELETE => types::Method::Delete,
                &hyper::Method::PATCH => types::Method::Patch,
                &hyper::Method::HEAD => types::Method::Head,
                &hyper::Method::OPTIONS => types::Method::Options,
                request => types::Method::Other(request.as_str().into()),
            },
            scheme: request.uri().scheme().map(|scheme| match scheme.as_str() {
                "http" => types::Scheme::Http,
                "https" => types::Scheme::Https,
                _ => types::Scheme::Other(scheme.as_str().into()),
            }),
            path_with_query: request.uri().path_and_query().map(|p| p.as_str().into()),
            authority: request.uri().authority().map(|a| a.as_str().into()),
            headers: types::Fields(
                request
                    .headers()
                    .iter()
                    .map(|(k, v)| (k.as_str().into(), v.as_bytes().into()))
                    .collect(),
            ),
            body: types::Body {
                stream: Some(request_body_rx),
                trailers: Some(request_trailers_rx),
            },
            options: None,
        })?;

        drop(wasi_http);

        let mut promises = PromisesUnordered::new();

        read::<F>(
            &mut store,
            &mut promises,
            request.into_body(),
            request_body_tx,
            request_trailers_tx,
        )
        .await?;

        let proxy = proxy::Proxy::new(&mut store, &instance)?;

        promises.push(
            proxy
                .wasi_http_handler()
                .call_handle(&mut store, my_request)
                .await?
                .map(Event::Response),
        );

        return poll::<F>(store, promises, None).await.map(|v| v.unwrap());
    }
}

async fn read<F: RuntimeFactors>(
    store: &mut Store<F::InstanceState>,
    promises: &mut PromisesUnordered<Event>,
    mut body: Body,
    body_tx: StreamWriter<Cursor<Bytes>>,
    trailers_tx: FutureWriter<Resource<types::Fields>>,
) -> Result<()> {
    if let Some(frame) = body.frame().await {
        match frame?.into_data() {
            Ok(chunk) => promises.push(
                body_tx
                    .write(chunk)
                    .map(move |tx| Event::RequestBodyWrite(body, tx, trailers_tx)),
            ),

            Err(frame) => match frame.into_trailers() {
                Ok(trailers) => {
                    drop(body_tx);

                    let mut wasi_http = OutboundHttpFactor::get_wasi_http_draft_impl(
                        store.data_mut().factors_instance_state_mut(),
                    )
                    .context("missing OutboundHttpFactor")?;

                    let trailers = wasi_http.table().push(types::Fields(
                        trailers
                            .iter()
                            .map(|(k, v)| (k.as_str().into(), v.as_bytes().into()))
                            .collect(),
                    ))?;

                    drop(wasi_http);

                    promises.push(
                        trailers_tx
                            .write(trailers)
                            .map(|_| Event::RequestTrailersWrite),
                    )
                }
                Err(_) => unreachable!(),
            },
        }
    }

    Ok(())
}

#[allow(clippy::manual_async_fn)]
fn poll<F: RuntimeFactors>(
    mut store: Store<F::InstanceState>,
    mut promises: PromisesUnordered<Event>,
    mut body_tx: Option<mpsc::Sender<Result<Frame<Bytes>, ErrorCode>>>,
) -> impl Future<Output = Result<Option<Response<HyperOutgoingBody>>>> + Send {
    async move {
        while let Some(event) = promises.next(&mut store).await? {
            match event {
                Event::RequestBodyWrite(body, body_tx, trailers_tx) => {
                    read::<F>(
                        &mut store,
                        &mut promises,
                        body,
                        body_tx
                            .ok_or_else(|| anyhow!("request body reader closed unexpectedly"))?,
                        trailers_tx,
                    )
                    .await?;
                }
                Event::RequestTrailersWrite => {}
                Event::Response(response) => {
                    let mut wasi_http = OutboundHttpFactor::get_wasi_http_draft_impl(
                        store.data_mut().factors_instance_state_mut(),
                    )
                    .context("missing OutboundHttpFactor")?;

                    let mut response = wasi_http.table().delete(response?)?;

                    drop(wasi_http);

                    let (response_body_tx, response_body_rx) = mpsc::channel(1);

                    if let Some(mut body) = response.body.take() {
                        let trailers = body.trailers.take();

                        promises.push(
                            body.stream
                                .take()
                                .unwrap()
                                .read()
                                .map(move |rx| Event::ResponseBodyRead(rx, trailers)),
                        );

                        tokio::task::spawn(poll::<F>(store, promises, Some(response_body_tx)).map(
                            |result| {
                                if let Err(e) = result {
                                    tracing::warn!("error sending response body: {e:?}");
                                }
                            },
                        ));
                    }

                    let mut builder = hyper::Response::builder().status(response.status_code);
                    for (k, v) in response.headers.0 {
                        builder = builder.header(k, v);
                    }
                    return Ok(Some(
                        builder.body(BoxBody::new(StreamBody::new(response_body_rx)))?,
                    ));
                }
                Event::ResponseBodyRead(Ok((rx, chunk)), trailers) => {
                    body_tx
                        .as_mut()
                        .unwrap()
                        .send(Ok(Frame::data(chunk)))
                        .await?;

                    promises.push(
                        rx.read()
                            .map(move |rx| Event::ResponseBodyRead(rx, trailers)),
                    );
                }
                Event::ResponseBodyRead(Err(_), trailers) => {
                    if let Some(trailers) = trailers {
                        promises.push(trailers.read().map(Event::ResponseTrailersRead));
                    }
                }
                Event::ResponseTrailersRead(trailers) => {
                    if let Ok(trailers) = trailers {
                        let mut wasi_http = OutboundHttpFactor::get_wasi_http_draft_impl(
                            store.data_mut().factors_instance_state_mut(),
                        )
                        .context("missing OutboundHttpFactor")?;

                        body_tx
                            .as_mut()
                            .unwrap()
                            .send(Ok(Frame::trailers(
                                wasi_http
                                    .table()
                                    .delete(trailers)?
                                    .0
                                    .into_iter()
                                    .map(|(k, v)| Ok((k.try_into()?, v.try_into()?)))
                                    .collect::<Result<_>>()?,
                            )))
                            .await?;
                    }
                }
            }
        }

        Ok(None)
    }
}

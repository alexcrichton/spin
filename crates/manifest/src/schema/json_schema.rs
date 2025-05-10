use crate::schema::v2::{ComponentSpec, Map, OneOrManyComponentSpecs};
use schemars::JsonSchema;

// The structs here allow dead code because they exist only
// to represent JSON schemas, and are never instantiated.

#[allow(dead_code)]
#[derive(JsonSchema)]
pub struct TriggerSchema {
    /// HTTP triggers
    #[schemars(default)]
    http: Vec<HttpTriggerSchema>,
    /// Redis triggers
    #[schemars(default)]
    redis: Vec<RedisTriggerSchema>,
}

#[allow(dead_code)]
#[derive(JsonSchema)]
#[schemars(deny_unknown_fields)]
pub struct HttpTriggerSchema {
    /// `id = "trigger-id"`
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub id: String,
    /// `component = ...`
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub component: Option<ComponentSpec>,
    /// `components = { ... }`
    #[serde(default, skip_serializing_if = "Map::is_empty")]
    pub components: Map<String, OneOrManyComponentSpecs>,
    /// `route = "/user/:name/..."`
    route: HttpRouteSchema,
    /// `executor = { type = "wagi" }
    #[schemars(default, schema_with = "toml_table")]
    executor: Option<toml::Table>,
}

#[allow(dead_code)]
#[derive(JsonSchema)]
#[schemars(untagged)]
pub enum HttpRouteSchema {
    /// `route = "/user/:name/..."`
    Route(String),
    /// `route = { private = true }`
    Private(HttpPrivateEndpoint),
}

#[allow(dead_code)]
#[derive(JsonSchema)]
#[schemars(deny_unknown_fields)]
pub struct HttpPrivateEndpoint {
    /// Whether the private endpoint is private. This must be true.
    pub private: bool,
}

#[allow(dead_code)]
#[derive(JsonSchema)]
#[schemars(deny_unknown_fields)]
pub struct RedisTriggerSchema {
    /// `id = "trigger-id"`
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub id: String,
    /// `component = ...`
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub component: Option<ComponentSpec>,
    /// `components = { ... }`
    #[serde(default, skip_serializing_if = "Map::is_empty")]
    pub components: Map<String, OneOrManyComponentSpecs>,
    /// `channel = "my-messages"`
    channel: String,
    /// `address = "redis://redis.example.com:6379"`
    #[serde(default, skip_serializing_if = "Option::is_none")]
    address: Option<String>,
}

pub fn toml_table(_gen: &mut schemars::gen::SchemaGenerator) -> schemars::schema::Schema {
    schemars::schema::Schema::Object(schemars::schema::SchemaObject {
        instance_type: Some(schemars::schema::SingleOrVec::Single(Box::new(
            schemars::schema::InstanceType::Object,
        ))),
        ..Default::default()
    })
}

pub fn map_of_toml_tables(_gen: &mut schemars::gen::SchemaGenerator) -> schemars::schema::Schema {
    schemars::schema::Schema::Object(schemars::schema::SchemaObject {
        instance_type: Some(schemars::schema::SingleOrVec::Single(Box::new(
            schemars::schema::InstanceType::Object,
        ))),
        ..Default::default()
    })
}

pub fn one_or_many<T: schemars::JsonSchema>(
    gen: &mut schemars::gen::SchemaGenerator,
) -> schemars::schema::Schema {
    schemars::schema::Schema::Object(schemars::schema::SchemaObject {
        subschemas: Some(Box::new(schemars::schema::SubschemaValidation {
            one_of: Some(vec![
                gen.subschema_for::<T>(),
                gen.subschema_for::<Vec<T>>(),
            ]),
            ..Default::default()
        })),
        ..Default::default()
    })
}

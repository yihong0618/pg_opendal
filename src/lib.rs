use anyhow::Result;
use opendal::Operator;
use opendal::Scheme;
use pgrx::prelude::*;
use pgrx::JsonB;
use serde_json::Value;
use std::collections::HashMap;
use std::str::FromStr;
use tokio::runtime::Runtime;
use futures::stream::TryStreamExt;

pgrx::pg_module_magic!();

async fn do_read_async(op: Operator, path: &str) -> Result<String, String> {
    match op.read(path).await {
        Ok(data) => String::from_utf8(data.to_vec())
            .map_err(|e| format!("Failed to convert data to UTF-8: {}", e)),
        Err(e) => Err(format!("Failed to read file '{}': {}", path, e)),
    }
}

#[pg_extern]
fn pg_opendal_read(service: &str, path: &str, config: JsonB) -> Result<String, String> {
    let config_map = jsonb_to_hashmap(config.0)
        .map_err(|e| format!("Failed to parse config: {}", e))?;
    let op = create_operator(service, config_map)
        .map_err(|e| format!("Failed to create operator: {}", e))?;
    
    let rt = Runtime::new().map_err(|e| format!("Failed to create Tokio runtime: {}", e))?;
    rt.block_on(do_read_async(op, path))
}

async fn do_write_async(op: Operator, path: &str, content: &[u8]) -> Result<bool, String> {
    op.write(path, content.to_owned())
        .await
        .map(|_| true)
        .map_err(|e| format!("Failed to write to '{}': {}", path, e))
}

#[pg_extern]
fn pg_opendal_write(service: &str, path: &str, content: &str, config: JsonB) -> Result<bool, String> {
    let config_map = jsonb_to_hashmap(config.0)
        .map_err(|e| format!("Failed to parse config: {}", e))?;
    let op = create_operator(service, config_map)
        .map_err(|e| format!("Failed to create operator: {}", e))?;

    let rt = Runtime::new().map_err(|e| format!("Failed to create Tokio runtime: {}", e))?;
    rt.block_on(do_write_async(op, path, content.as_bytes()))
}

async fn do_exists_async(op: Operator, path: &str) -> Result<bool, String> {
    match op.stat(path).await {
        Ok(_) => Ok(true),
        Err(e) => {
            if e.kind() == opendal::ErrorKind::NotFound {
                Ok(false)
            } else {
                Err(format!("Failed to check existence of '{}': {}", path, e))
            }
        }
    }
}

#[pg_extern]
fn pg_opendal_exists(service: &str, path: &str, config: JsonB) -> Result<bool, String> {
    let config_map = jsonb_to_hashmap(config.0)
        .map_err(|e| format!("Failed to parse config: {}", e))?;
    let op = create_operator(service, config_map)
        .map_err(|e| format!("Failed to create operator: {}", e))?;

    let rt = Runtime::new().map_err(|e| format!("Failed to create Tokio runtime: {}", e))?;
    rt.block_on(do_exists_async(op, path))
}

async fn do_delete_async(op: Operator, path: &str) -> Result<bool, String> {
    op.delete(path)
        .await
        .map(|_| true)
        .map_err(|e| format!("Failed to delete '{}': {}", path, e))
}

#[pg_extern]
fn pg_opendal_delete(service: &str, path: &str, config: JsonB) -> Result<bool, String> {
    let config_map = jsonb_to_hashmap(config.0)
        .map_err(|e| format!("Failed to parse config: {}", e))?;
    let op = create_operator(service, config_map)
        .map_err(|e| format!("Failed to create operator: {}", e))?;

    let rt = Runtime::new().map_err(|e| format!("Failed to create Tokio runtime: {}", e))?;
    rt.block_on(do_delete_async(op, path))
}

async fn do_stat_async(op: Operator, path: &str) -> Result<JsonB, String> {
    match op.stat(path).await {
        Ok(metadata) => {
            let mut stat_info = serde_json::Map::new();
            stat_info.insert(
                "content_length".to_string(),
                Value::Number(serde_json::Number::from(metadata.content_length())),
            );
            stat_info.insert("is_file".to_string(), Value::Bool(metadata.is_file()));
            stat_info.insert("is_dir".to_string(), Value::Bool(metadata.is_dir()));

            if let Some(last_modified) = metadata.last_modified() {
                stat_info.insert(
                    "last_modified".to_string(),
                    Value::String(last_modified.to_rfc3339()),
                );
            }

            Ok(JsonB(Value::Object(stat_info)))
        }
        Err(e) => Err(format!("Failed to get stat for '{}': {}", path, e)),
    }
}

#[pg_extern]
fn pg_opendal_stat(service: &str, path: &str, config: JsonB) -> Result<JsonB, String> {
    let config_map = jsonb_to_hashmap(config.0)
        .map_err(|e| format!("Failed to parse config: {}", e))?;
    let op = create_operator(service, config_map)
        .map_err(|e| format!("Failed to create operator: {}", e))?;
    
    let rt = Runtime::new().map_err(|e| format!("Failed to create Tokio runtime: {}", e))?;
    rt.block_on(do_stat_async(op, path))
}

async fn do_create_dir_async(op: Operator, path: &str) -> Result<bool, String> {
    op.create_dir(path)
        .await
        .map(|_| true)
        .map_err(|e| format!("Failed to create directory '{}': {}", path, e))
}

#[pg_extern]
fn pg_opendal_create_dir(service: &str, path: &str, config: JsonB) -> Result<bool, String> {
    let config_map = jsonb_to_hashmap(config.0)
        .map_err(|e| format!("Failed to parse config: {}", e))?;
    let op = create_operator(service, config_map)
        .map_err(|e| format!("Failed to create operator: {}", e))?;

    let rt = Runtime::new().map_err(|e| format!("Failed to create Tokio runtime: {}", e))?;
    rt.block_on(do_create_dir_async(op, path))
}

async fn do_copy_async(op: Operator, source: &str, target: &str) -> Result<bool, String> {
    op.copy(source, target)
        .await
        .map(|_| true)
        .map_err(|e| format!("Failed to copy from '{}' to '{}': {}", source, target, e))
}

#[pg_extern]
fn pg_opendal_copy(service: &str, source: &str, target: &str, config: JsonB) -> Result<bool, String> {
    let config_map = jsonb_to_hashmap(config.0)
        .map_err(|e| format!("Failed to parse config: {}", e))?;
    let op = create_operator(service, config_map)
        .map_err(|e| format!("Failed to create operator: {}", e))?;

    let rt = Runtime::new().map_err(|e| format!("Failed to create Tokio runtime: {}", e))?;
    rt.block_on(do_copy_async(op, source, target))
}

async fn do_rename_async(op: Operator, source: &str, target: &str) -> Result<bool, String> {
    op.rename(source, target)
        .await
        .map(|_| true)
        .map_err(|e| format!("Failed to rename from '{}' to '{}': {}", source, target, e))
}

#[pg_extern]
fn pg_opendal_rename(service: &str, source: &str, target: &str, config: JsonB) -> Result<bool, String> {
    let config_map = jsonb_to_hashmap(config.0)
        .map_err(|e| format!("Failed to parse config: {}", e))?;
    let op = create_operator(service, config_map)
        .map_err(|e| format!("Failed to create operator: {}", e))?;

    let rt = Runtime::new().map_err(|e| format!("Failed to create Tokio runtime: {}", e))?;
    rt.block_on(do_rename_async(op, source, target))
}

async fn do_list_async(op: Operator, path: &str) -> Result<Vec<JsonB>, String> {
    let mut lister = op.lister(path).await // op.lister() is async for the OpenDAL version in use
        .map_err(|e| format!("Failed to get lister for '{}': {}", path, e))?;
    
    let mut results = Vec::new();
    
    while let Some(entry_result) = lister.try_next().await
        .map_err(|e| format!("Failed to list contents of '{}': {}", path, e))? {
        // entry_result is an opendal::Entry
        let entry = entry_result; // Assuming entry_result is the Entry itself after try_next handles Result
        let mut entry_info = serde_json::Map::new();
        entry_info.insert("name".to_string(), Value::String(entry.name().to_string()));
        entry_info.insert("path".to_string(), Value::String(entry.path().to_string()));

        // Fetch metadata for each entry asynchronously
        let metadata = op.stat(entry.path()).await
            .map_err(|e| format!("Failed to get metadata for entry '{}': {}", entry.path(), e))?;
        
        entry_info.insert("is_file".to_string(), Value::Bool(metadata.is_file()));
        entry_info.insert("is_dir".to_string(), Value::Bool(metadata.is_dir()));
        entry_info.insert(
            "content_length".to_string(),
            Value::Number(serde_json::Number::from(metadata.content_length())),
        );


        if let Some(last_modified) = metadata.last_modified() {
            entry_info.insert(
                "last_modified".to_string(),
                Value::String(last_modified.to_rfc3339()),
            );
        }
        results.push(JsonB(Value::Object(entry_info)));
    }
    Ok(results)
}

#[pg_extern]
fn pg_opendal_list(service: &str, path: &str, config: JsonB) -> Result<Vec<JsonB>, String> {
    let config_map = jsonb_to_hashmap(config.0)
        .map_err(|e| format!("Failed to parse config: {}", e))?;
    let op = create_operator(service, config_map)
        .map_err(|e| format!("Failed to create operator: {}", e))?;
    
    let rt = Runtime::new().map_err(|e| format!("Failed to create Tokio runtime: {}", e))?;
    rt.block_on(do_list_async(op, path))
}

#[pg_extern]
fn pg_opendal_capability(service: &str, config: JsonB) -> Result<JsonB, String> {
    let config_map = jsonb_to_hashmap(config.0)
        .map_err(|e| format!("Failed to parse config: {}", e))?;
    let op = create_operator(service, config_map)
        .map_err(|e| format!("Failed to create operator: {}", e))?;

    let capability = op.info().full_capability();
    let mut cap_info = serde_json::Map::new();

    cap_info.insert("read".to_string(), Value::Bool(capability.read));
    cap_info.insert("write".to_string(), Value::Bool(capability.write));
    cap_info.insert("list".to_string(), Value::Bool(capability.list));
    cap_info.insert("stat".to_string(), Value::Bool(capability.stat));
    cap_info.insert("delete".to_string(), Value::Bool(capability.delete));
    cap_info.insert("copy".to_string(), Value::Bool(capability.copy));
    cap_info.insert("rename".to_string(), Value::Bool(capability.rename));
    cap_info.insert("create_dir".to_string(), Value::Bool(capability.create_dir));

    Ok(JsonB(Value::Object(cap_info)))
}

fn jsonb_to_hashmap(value: Value) -> Result<HashMap<String, String>> {
    let mut map = HashMap::new();
    if let Value::Object(obj) = value {
        for (k, v) in obj {
            if let Value::String(s) = v {
                map.insert(k, s);
            } else {
                return Err(anyhow::anyhow!("Config values must be strings"));
            }
        }
        Ok(map)
    } else {
        Err(anyhow::anyhow!("Config must be a JSON object"))
    }
}

fn create_operator(service: &str, config: HashMap<String, String>) -> Result<Operator> {
    let scheme = Scheme::from_str(service)
        .map_err(|e| anyhow::anyhow!("Invalid service type '{}': {}", service, e))?;
    opendal::Operator::via_iter(scheme, config).map_err(|e| anyhow::anyhow!(e))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_json_conversion() {
        let json = serde_json::json!({ "bucket": "my-bucket" });
        let map = jsonb_to_hashmap(json).unwrap();
        assert_eq!(map.get("bucket"), Some(&"my-bucket".to_string()));
    }
}

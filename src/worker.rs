use std::path::PathBuf;
use crate::types::FleetState;

pub async fn get_fleet(worker_url: &str, token: &str) -> Result<FleetState, String> {
    let client = reqwest::Client::new();
    let resp = client
        .get(format!("{}/fleet", worker_url))
        .header("Authorization", format!("Bearer {}", token))
        .send()
        .await
        .map_err(|e| e.to_string())?;

    if !resp.status().is_success() {
        return Err(format!("HTTP {}", resp.status()));
    }

    resp.json::<FleetState>().await.map_err(|e| e.to_string())
}

pub async fn save_fleet(state: FleetState, worker_url: String, token: String) -> Result<(), String> {
    let client = reqwest::Client::new();
    let resp = client
        .put(format!("{}/fleet", worker_url))
        .header("Authorization", format!("Bearer {}", token))
        .json(&state)
        .send()
        .await
        .map_err(|e| e.to_string())?;

    if !resp.status().is_success() {
        return Err(format!("HTTP {} saving fleet", resp.status()));
    }

    Ok(())
}

pub async fn upload_firmware(
    bin_path: PathBuf,
    device_id: String,
    version: String,
    worker_url: String,
    token: String,
) -> Result<String, String> {
    let bytes = tokio::fs::read(&bin_path).await.map_err(|e| e.to_string())?;
    let filename = format!("{}_v{}.bin", device_id, version);

    let part = reqwest::multipart::Part::bytes(bytes)
        .file_name(filename.clone())
        .mime_str("application/octet-stream")
        .map_err(|e| e.to_string())?;

    let form = reqwest::multipart::Form::new()
        .text("deviceId", device_id)
        .text("version", version)
        .part("file", part);

    let resp: serde_json::Value = reqwest::Client::new()
        .post(format!("{}/upload", worker_url))
        .header("Authorization", format!("Bearer {}", token))
        .multipart(form)
        .send()
        .await
        .map_err(|e| e.to_string())?
        .json()
        .await
        .map_err(|e| e.to_string())?;

    resp["url"]
        .as_str()
        .ok_or_else(|| format!("No url in response: {:?}", resp))
        .map(|s| s.to_string())
}

pub async fn delete_firmware(filename: String, worker_url: String, token: String) -> Result<(), String> {
    reqwest::Client::new()
        .delete(format!("{}/firmware/{}", worker_url, filename))
        .header("Authorization", format!("Bearer {}", token))
        .send()
        .await
        .map_err(|e| e.to_string())?;
    Ok(())
}

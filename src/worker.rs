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
    vps_host: String,
    vps_key: String,
    vps_firmware_dir: String,
    vps_firmware_url: String,
) -> Result<String, String> {
    let filename = format!("{}_v{}.bin", device_id, version);
    let bytes = tokio::fs::read(&bin_path).await.map_err(|e| e.to_string())?;

    // 1. Upload to Cloudflare R2 for fleet records and backup storage
    let part = reqwest::multipart::Part::bytes(bytes)
        .file_name(filename.clone())
        .mime_str("application/octet-stream")
        .map_err(|e| e.to_string())?;

    let form = reqwest::multipart::Form::new()
        .text("deviceId", device_id)
        .text("version", version)
        .part("file", part);

    let http_resp = reqwest::Client::new()
        .post(format!("{}/upload", worker_url))
        .header("Authorization", format!("Bearer {}", token))
        .multipart(form)
        .send()
        .await
        .map_err(|e| e.to_string())?;

    if !http_resp.status().is_success() {
        return Err(format!("HTTP {} uploading firmware", http_resp.status()));
    }

    let cf_resp: serde_json::Value = http_resp.json().await.map_err(|e| e.to_string())?;
    let cf_url = cf_resp["url"]
        .as_str()
        .ok_or_else(|| format!("No url in Cloudflare response: {:?}", cf_resp))?
        .to_string();

    // 2. SCP to VPS for reliable HTTP delivery to ESP32.
    //    ESP32 TLS times out on large binaries downloaded over HTTPS from Cloudflare.
    if !vps_host.is_empty() && !vps_key.is_empty() {
        let remote = format!("{}:{}/{}", vps_host, vps_firmware_dir, filename);
        let bin_str = bin_path.to_string_lossy().to_string();
        let key = vps_key.clone();
        let out = tokio::task::spawn_blocking(move || {
            std::process::Command::new("scp")
                .args([
                    "-i", &key,
                    "-o", "StrictHostKeyChecking=no",
                    "-o", "BatchMode=yes",
                    &bin_str,
                    &remote,
                ])
                .output()
        })
        .await
        .map_err(|e| e.to_string())?
        .map_err(|e| format!("scp launch failed: {}", e))?;

        if !out.status.success() {
            return Err(format!(
                "SCP to VPS failed: {}",
                String::from_utf8_lossy(&out.stderr).trim()
            ));
        }

        // Return VPS HTTP URL — no TLS, works reliably with ESP32 HTTPUpdate
        return Ok(format!("{}/{}", vps_firmware_url.trim_end_matches('/'), filename));
    }

    // Fallback: VPS not configured, use Cloudflare URL
    Ok(cf_url)
}

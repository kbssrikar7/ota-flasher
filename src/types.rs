use serde::{Deserialize, Serialize};
use std::path::PathBuf;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Topics {
    pub cmd: String,
    pub status: String,
    pub ota: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Device {
    pub id: String,
    pub name: String,
    #[serde(default)]
    pub company: String,
    #[serde(default)]
    pub sketch_dir: String,
    pub topics: Topics,
    #[serde(default)]
    pub desired_version: String,
    #[serde(default)]
    pub last_deploy_by: String,
    #[serde(default)]
    pub added_by: String,
    #[serde(default)]
    pub added_at: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct DeployRecord {
    pub device_id: String,
    pub device_name: String,
    pub version: String,
    pub deployed_by: String,
    pub deployed_at: String,
    #[serde(default)]
    pub firmware_url: String,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct FleetState {
    pub devices: Vec<Device>,
    pub deploy_history: Vec<DeployRecord>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AppConfig {
    pub worker_url: String,
    pub worker_token: String,
    pub sketch_root: String,
    pub fqbn: String,
    pub mqtt_host: String,
    pub mqtt_port: u16,
    pub mqtt_user: String,
    pub mqtt_pass: String,
}

impl Default for AppConfig {
    fn default() -> Self {
        Self {
            worker_url: String::new(),
            worker_token: String::new(),
            sketch_root: dirs::home_dir()
                .map(|h| h.join("Arduino").to_string_lossy().to_string())
                .unwrap_or_default(),
            fqbn: "esp32:esp32:esp32".to_string(),
            mqtt_host: "mqtt.vitalitysoft.com".to_string(),
            mqtt_port: 8883,
            mqtt_user: "solar".to_string(),
            mqtt_pass: String::new(),
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub enum LogLevel {
    Normal,
    Info,
    Ok,
    Error,
}

#[derive(Debug, Clone)]
pub enum AppEvent {
    MqttConnected,
    MqttDisconnected,
    MqttStatus { device_id: String, status: String },
    FleetLoaded(FleetState),
    FleetSaved,
    CompileOutput { line: String, level: LogLevel },
    CompileDone { success: bool, bin_path: Option<PathBuf> },
    UploadDone { url: String },
    OtaPublished,
    FolderPicked { context: FolderPickCtx, path: PathBuf },
    Error(String),
}

#[derive(Debug, Clone)]
pub enum FolderPickCtx {
    DeploySketch,
    AddDeviceSketch,
    SettingsSketchRoot,
}

#[derive(Debug, Clone, PartialEq)]
pub enum DeployPhase {
    Form,
    Compiling,
    Uploading,
    Publishing,
    Waiting,
    Done,
    Failed(String),
}

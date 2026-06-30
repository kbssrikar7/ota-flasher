use std::{
    collections::{HashMap, HashSet},
    path::PathBuf,
    sync::{Arc, Mutex, mpsc, atomic::{AtomicBool, AtomicU64, Ordering}},
    time::Instant,
};
use egui::{Color32, FontFamily, FontId, RichText, Stroke, Margin, Rounding};
use rumqttc::{Client, QoS};
use crate::{
    compile,
    config::{load_config, save_config},
    mqtt::run_mqtt,
    types::*,
    worker,
};

// ─── Palette ─────────────────────────────────────────────────────────────────
const BG: Color32      = Color32::from_rgb(10,  12,  16);
const SURFACE: Color32 = Color32::from_rgb(18,  21,  28);
const CARD: Color32    = Color32::from_rgb(22,  26,  34);
const BORDER: Color32  = Color32::from_rgb(38,  44,  56);
const PRIMARY: Color32 = Color32::from_rgb(79, 142, 247);
const SUCCESS: Color32 = Color32::from_rgb(52, 199, 110);
const WARNING: Color32 = Color32::from_rgb(245, 166,  35);
const DANGER: Color32  = Color32::from_rgb(224,  82,  82);
const TEXT: Color32    = Color32::from_rgb(220, 224, 232);
const MUTED: Color32   = Color32::from_rgb(100, 108, 124);

// ─── Local structs ───────────────────────────────────────────────────────────

#[derive(Default, PartialEq, Clone, Copy)]
enum Tab { #[default] Devices, History }

struct DeployState {
    device_id: String,
    device_name: String,
    deployer_name: String,
    new_version: String,
    sketch_dir: String,
    phase: DeployPhase,
    log_lines: Vec<(String, LogLevel)>,
    bin_path: Option<PathBuf>,
    firmware_url: Option<String>,
}

#[derive(Default)]
struct AddForm {
    by_name: String,
    company: String,
    device_name: String,
    device_id: String,
    sketch_dir: String,
    version: String,
    tags_input: String,
    error: String,
}

struct Notif {
    msg: String,
    is_err: bool,
    at: Instant,
}

// ─── App ─────────────────────────────────────────────────────────────────────

pub struct App {
    // ── State
    config: AppConfig,
    fleet: FleetState,
    fleet_loaded: bool,
    mqtt_connected: bool,
    mqtt_client: Arc<Mutex<Option<Client>>>,
    mqtt_status: HashMap<String, String>,

    // ── Channels & runtime
    event_tx: mpsc::Sender<AppEvent>,
    event_rx: mpsc::Receiver<AppEvent>,
    rt: Arc<tokio::runtime::Runtime>,
    egui_ctx: egui::Context,

    // ── UI state
    tab: Tab,
    deploy: Option<DeployState>,
    show_add: bool,
    add_form: AddForm,
    show_settings: bool,
    settings_buf: AppConfig,
    notif: Option<Notif>,
    picking_folder: Arc<AtomicBool>,
    mqtt_gen: Arc<AtomicU64>,
    first_run: bool,
    search_query: String,
    selected: HashSet<String>,
    bulk_deploy: Option<BulkDeployState>,
}

impl App {
    pub fn new(cc: &eframe::CreationContext<'_>, rt: Arc<tokio::runtime::Runtime>) -> Self {
        let config = load_config();
        let (tx, rx) = mpsc::channel();
        let mqtt_client = Arc::new(Mutex::new(None::<Client>));
        let ctx = cc.egui_ctx.clone();

        let needs_setup = config.worker_url.is_empty()
            || config.worker_token.is_empty()
            || config.mqtt_pass.is_empty();

        let app = Self {
            settings_buf: config.clone(),
            config: config.clone(),
            fleet: FleetState::default(),
            fleet_loaded: false,
            mqtt_connected: false,
            mqtt_client: mqtt_client.clone(),
            mqtt_status: HashMap::new(),
            event_tx: tx.clone(),
            event_rx: rx,
            rt: rt.clone(),
            egui_ctx: ctx.clone(),
            tab: Tab::Devices,
            deploy: None,
            show_add: false,
            add_form: AddForm::default(),
            show_settings: needs_setup,
            notif: None,
            picking_folder: Arc::new(AtomicBool::new(false)),
            mqtt_gen: Arc::new(AtomicU64::new(0)),
            first_run: needs_setup,
            search_query: String::new(),
            selected: HashSet::new(),
            bulk_deploy: None,
        };

        // Start MQTT
        let cfg = config.clone();
        let arc = mqtt_client;
        let tx2 = tx.clone();
        let ctx2 = ctx.clone();
        std::thread::spawn(move || run_mqtt(cfg, arc, tx2, ctx2));

        // Load fleet if worker is configured
        if !config.worker_url.is_empty() {
            app.load_fleet();
        }

        app
    }

    // ── Background operations ─────────────────────────────────────────────────

    fn load_fleet(&self) {
        let tx = self.event_tx.clone();
        let ctx = self.egui_ctx.clone();
        let url = self.config.worker_url.clone();
        let tok = self.config.worker_token.clone();
        self.rt.spawn(async move {
            match worker::get_fleet(&url, &tok).await {
                Ok(f)  => { tx.send(AppEvent::FleetLoaded(f)).ok(); }
                Err(e) => { tx.send(AppEvent::Error(format!("Fleet load: {}", e))).ok(); }
            }
            ctx.request_repaint();
        });
    }

    fn save_fleet(&self) {
        let tx = self.event_tx.clone();
        let ctx = self.egui_ctx.clone();
        let fleet = self.fleet.clone();
        let url = self.config.worker_url.clone();
        let tok = self.config.worker_token.clone();
        self.rt.spawn(async move {
            match worker::save_fleet(fleet, url, tok).await {
                Ok(()) => { tx.send(AppEvent::FleetSaved).ok(); }
                Err(e) => { tx.send(AppEvent::Error(format!("Fleet save: {}", e))).ok(); }
            }
            ctx.request_repaint();
        });
    }

    fn open_deploy(&mut self, device: &Device) {
        let sketch_dir = if device.sketch_dir.is_empty() {
            default_sketch_dir(&self.config.sketch_root, &device.id)
        } else {
            resolve_sketch_dir(&self.config.sketch_root, &device.sketch_dir)
        };
        let current = compile::read_firmware_version(
            &std::path::Path::new(&sketch_dir).join(format!("{}.ino", std::path::Path::new(&sketch_dir).file_name().map(|n| n.to_string_lossy().to_string()).unwrap_or_default()))
        ).unwrap_or_else(|| device.desired_version.clone());
        let next = bump_patch(&current);

        self.deploy = Some(DeployState {
            device_id: device.id.clone(),
            device_name: device.name.clone(),
            deployer_name: String::new(),
            new_version: next,
            sketch_dir,
            phase: DeployPhase::Form,
            log_lines: Vec::new(),
            bin_path: None,
            firmware_url: None,
        });
    }

    fn start_deploy(&mut self) {
        let d = match &self.deploy {
            Some(d) if d.phase == DeployPhase::Form => d,
            _ => return,
        };

        if d.deployer_name.trim().is_empty() || d.new_version.trim().is_empty() {
            return;
        }

        let sketch = d.sketch_dir.clone();
        let ver    = d.new_version.clone();
        let fqbn   = self.config.fqbn.clone();
        let tx     = self.event_tx.clone();
        let ctx    = self.egui_ctx.clone();

        if let Some(d) = &mut self.deploy {
            d.phase = DeployPhase::Compiling;
            d.log_lines.clear();
        }

        std::thread::spawn(move || compile::compile_sketch(sketch, ver, fqbn, tx, ctx));
    }

    fn pick_folder(&mut self, ctx_field: FolderPickCtx) {
        if self.picking_folder.load(Ordering::SeqCst) { return; }
        self.picking_folder.store(true, Ordering::SeqCst);

        let flag = self.picking_folder.clone();
        let tx   = self.event_tx.clone();
        let ctx  = self.egui_ctx.clone();
        std::thread::spawn(move || {
            let picked = rfd::FileDialog::new()
                .set_title("Select folder")
                .pick_folder();
            flag.store(false, Ordering::SeqCst); // always reset, even on cancel
            if let Some(path) = picked {
                tx.send(AppEvent::FolderPicked { context: ctx_field, path }).ok();
                ctx.request_repaint();
            }
        });
    }

    fn mqtt_publish(&self, topic: &str, payload: &[u8], retain: bool) {
        if let Some(client) = &*self.mqtt_client.lock().unwrap() {
            let _ = client.publish(topic, QoS::AtLeastOnce, retain, payload);
        }
    }

    fn notify(&mut self, msg: impl Into<String>, is_err: bool) {
        self.notif = Some(Notif { msg: msg.into(), is_err, at: Instant::now() });
    }

    fn open_bulk_deploy_form(&mut self) {
        if self.selected.is_empty() { return; }
        // Collect selected devices that exist in fleet
        let devices: Vec<&crate::types::Device> = self.fleet.devices.iter()
            .filter(|d| self.selected.contains(&d.id))
            .collect();
        if devices.is_empty() { return; }

        // Build per-device entries
        let dev_entries: Vec<DeviceBulkDeploy> = devices.iter().map(|d| DeviceBulkDeploy {
            device_id: d.id.clone(),
            device_name: d.name.clone(),
            sketch_dir: if d.sketch_dir.is_empty() {
                default_sketch_dir(&self.config.sketch_root, &d.id)
            } else {
                resolve_sketch_dir(&self.config.sketch_root, &d.sketch_dir)
            },
            phase: DeployPhase::Form,
            log: vec![],
        }).collect();

        // Use the first device's desired version as a suggestion
        let suggested_ver = devices.first()
            .and_then(|d| if d.desired_version.is_empty() { None } else { Some(bump_patch(&d.desired_version)) })
            .unwrap_or_else(|| "1.0.0".to_string());

        self.bulk_deploy = Some(BulkDeployState {
            deployer_name: String::new(),
            new_version: suggested_ver.clone(),
            devices: dev_entries,
            sketch_builds: HashMap::new(),
            form_deployer: String::new(),
            form_version: suggested_ver,
            show_form: true,
        });
    }

    fn start_bulk_deploy(&mut self) {
        let bd = match &mut self.bulk_deploy {
            Some(bd) => bd,
            None => return,
        };
        if bd.deployer_name.trim().is_empty() || bd.new_version.trim().is_empty() { return; }

        bd.show_form = false;
        let version = bd.new_version.clone();
        let fqbn    = self.config.fqbn.clone();

        // Group devices by sketch_dir, compile each unique sketch once
        let mut sketch_dirs: std::collections::HashSet<String> = std::collections::HashSet::new();
        for dev in &bd.devices {
            sketch_dirs.insert(dev.sketch_dir.clone());
        }

        for sketch_dir in sketch_dirs {
            bd.sketch_builds.insert(sketch_dir.clone(), SketchBuildState::Compiling);
            // Update all devices that use this sketch to Compiling
            for dev in bd.devices.iter_mut() {
                if dev.sketch_dir == sketch_dir {
                    dev.phase = DeployPhase::Compiling;
                }
            }
            let tx  = self.event_tx.clone();
            let ctx = self.egui_ctx.clone();
            let ver = version.clone();
            let fqbn2 = fqbn.clone();
            let sketch_dir2 = sketch_dir.clone();
            std::thread::spawn(move || {
                // Reuse compile_sketch but send BulkCompileDone instead of CompileDone
                // We need a wrapper that translates the events
                let (inner_tx, inner_rx) = std::sync::mpsc::channel::<AppEvent>();
                let inner_tx2 = inner_tx.clone();
                let inner_ctx = ctx.clone();
                std::thread::spawn(move || {
                    compile::compile_sketch(sketch_dir2.clone(), ver, fqbn2, inner_tx2, inner_ctx);
                });
                let mut bin_path: Option<PathBuf> = None;
                let mut success = false;
                for ev in inner_rx {
                    match ev {
                        AppEvent::CompileOutput { line, level } => {
                            tx.send(AppEvent::CompileOutput { line, level }).ok();
                        }
                        AppEvent::CompileDone { success: s, bin_path: bp } => {
                            success = s;
                            bin_path = bp;
                            break;
                        }
                        _ => {}
                    }
                }
                tx.send(AppEvent::BulkCompileDone { sketch_dir, success, bin_path }).ok();
                ctx.request_repaint();
            });
        }
    }

    // ── Event processing ──────────────────────────────────────────────────────

    fn handle_events(&mut self) {
        let events: Vec<AppEvent> = self.event_rx.try_iter().collect();
        for ev in events {
            match ev {
                AppEvent::MqttConnected => {
                    self.mqtt_connected = true;
                }
                AppEvent::MqttDisconnected => {
                    self.mqtt_connected = false;
                    let gen = self.mqtt_gen.fetch_add(1, Ordering::SeqCst) + 1;
                    let cfg     = self.config.clone();
                    let arc     = self.mqtt_client.clone();
                    let tx      = self.event_tx.clone();
                    let ctx     = self.egui_ctx.clone();
                    let gen_arc = self.mqtt_gen.clone();
                    std::thread::spawn(move || {
                        std::thread::sleep(std::time::Duration::from_secs(5));
                        // Only reconnect if we are still the latest generation
                        if gen_arc.load(Ordering::SeqCst) == gen {
                            run_mqtt(cfg, arc, tx, ctx);
                        }
                    });
                }
                AppEvent::MqttStatus { device_id, status } => {
                    self.mqtt_status.insert(device_id.clone(), status.clone());

                    // Check if this is a deploy confirmation
                    if let Some(d) = &mut self.deploy {
                        if d.phase == DeployPhase::Waiting && d.device_id == device_id {
                            let expected = format!("ONLINE v{}", d.new_version);
                            if status == expected || status.starts_with(&format!("{} ", expected)) {
                                d.phase = DeployPhase::Done;
                                d.log_lines.push((
                                    format!("✓ Device confirmed: {}", status),
                                    LogLevel::Ok,
                                ));
                                // Clear retained OTA trigger
                                let ota = format!("solar/{}/ota", device_id);
                                self.mqtt_publish(&ota, b"", true);
                            }
                        }
                    }
                }
                AppEvent::FleetLoaded(f) => {
                    self.fleet = f;
                    self.fleet_loaded = true;
                }
                AppEvent::FleetSaved => { /* silent */ }
                AppEvent::CompileOutput { line, level } => {
                    if let Some(d) = &mut self.deploy {
                        d.log_lines.push((line, level));
                    }
                }
                AppEvent::CompileDone { success, bin_path } => {
                    if let Some(d) = &mut self.deploy {
                        if success {
                            d.phase = DeployPhase::Uploading;
                            d.bin_path = bin_path.clone();
                            d.log_lines.push(("Uploading to Cloudflare R2 + VPS…".to_string(), LogLevel::Info));

                            let tx   = self.event_tx.clone();
                            let ctx  = self.egui_ctx.clone();
                            let bin  = bin_path.unwrap();
                            let did  = d.device_id.clone();
                            let ver  = d.new_version.clone();
                            let url  = self.config.worker_url.clone();
                            let tok  = self.config.worker_token.clone();
                            let vh   = self.config.vps_host.clone();
                            let vk   = self.config.vps_key.clone();
                            let vd   = self.config.vps_firmware_dir.clone();
                            let vu   = self.config.vps_firmware_url.clone();
                            self.rt.spawn(async move {
                                match worker::upload_firmware(bin, did, ver, url, tok, vh, vk, vd, vu).await {
                                    Ok(u)  => { tx.send(AppEvent::UploadDone { url: u }).ok(); }
                                    Err(e) => { tx.send(AppEvent::Error(format!("Upload: {}", e))).ok(); }
                                }
                                ctx.request_repaint();
                            });
                        } else {
                            d.phase = DeployPhase::Failed("Compilation failed".to_string());
                        }
                    }
                }
                AppEvent::UploadDone { url } => {
                    if let Some(d) = &mut self.deploy {
                        d.phase = DeployPhase::Publishing;
                        d.firmware_url = Some(url.clone());
                        d.log_lines.push((format!("✓ Uploaded → {}", url), LogLevel::Ok));
                        d.log_lines.push(("Publishing OTA trigger…".to_string(), LogLevel::Info));

                        let ota_topic = format!("solar/{}/ota", d.device_id);
                        self.mqtt_publish(&ota_topic, url.as_bytes(), true);
                        self.event_tx.send(AppEvent::OtaPublished).ok();
                    }
                }
                AppEvent::OtaPublished => {
                    if let Some(d) = &mut self.deploy {
                        d.phase = DeployPhase::Waiting;
                        d.log_lines.push(("✓ OTA trigger sent (retained) — waiting for device…".to_string(), LogLevel::Ok));

                        // Update fleet state
                        let did     = d.device_id.clone();
                        let ver     = d.new_version.clone();
                        let who     = d.deployer_name.clone();
                        let dname   = d.device_name.clone();
                        let fw_url  = d.firmware_url.clone().unwrap_or_default();
                        let now     = chrono::Utc::now().to_rfc3339();

                        for dev in &mut self.fleet.devices {
                            if dev.id == did {
                                dev.desired_version = ver.clone();
                                dev.last_deploy_by  = who.clone();
                            }
                        }
                        self.fleet.deploy_history.push(DeployRecord {
                            device_id:   did,
                            device_name: dname,
                            version:     ver,
                            deployed_by: who,
                            deployed_at: now,
                            firmware_url: fw_url,
                        });

                        self.save_fleet();
                    }
                }
                AppEvent::FolderPicked { context, path } => {
                    // flag already reset in the spawn thread; this is a no-op safety net
                    let s = path.to_string_lossy().to_string();
                    match context {
                        FolderPickCtx::DeploySketch => {
                            if let Some(d) = &mut self.deploy { d.sketch_dir = s; }
                        }
                        FolderPickCtx::AddDeviceSketch => {
                            self.add_form.sketch_dir = s;
                        }
                        FolderPickCtx::SettingsSketchRoot => {
                            self.settings_buf.sketch_root = s;
                        }
                    }
                }
                AppEvent::Error(e) => {
                    self.notify(e.clone(), true);
                    if let Some(d) = &mut self.deploy {
                        if !matches!(d.phase, DeployPhase::Form | DeployPhase::Done | DeployPhase::Failed(_)) {
                            d.phase = DeployPhase::Failed(e);
                        }
                    }
                    self.picking_folder.store(false, Ordering::SeqCst);
                }
                AppEvent::BulkCompileDone { sketch_dir, success, bin_path } => {
                    if let Some(bd) = &mut self.bulk_deploy {
                        if success {
                            if let Some(path) = bin_path {
                                bd.sketch_builds.insert(sketch_dir.clone(), SketchBuildState::Uploading);
                                for dev in bd.devices.iter_mut() {
                                    if dev.sketch_dir == sketch_dir {
                                        dev.phase = DeployPhase::Uploading;
                                    }
                                }
                                let tx2 = self.event_tx.clone();
                                let sketch_name = std::path::Path::new(&sketch_dir)
                                    .file_name()
                                    .map(|n| n.to_string_lossy().to_string())
                                    .unwrap_or_else(|| "sketch".to_string());
                                let version     = bd.new_version.clone();
                                let worker_url  = self.config.worker_url.clone();
                                let token       = self.config.worker_token.clone();
                                let vh          = self.config.vps_host.clone();
                                let vk          = self.config.vps_key.clone();
                                let vd          = self.config.vps_firmware_dir.clone();
                                let vu          = self.config.vps_firmware_url.clone();
                                let sketch_dir2 = sketch_dir.clone();
                                self.rt.spawn(async move {
                                    match worker::upload_firmware(path, sketch_name, version, worker_url, token, vh, vk, vd, vu).await {
                                        Ok(url) => { tx2.send(AppEvent::BulkUploadDone { sketch_dir: sketch_dir2, url }).ok(); }
                                        Err(e)  => { tx2.send(AppEvent::Error(format!("Bulk upload: {}", e))).ok(); }
                                    }
                                });
                            }
                        } else {
                            bd.sketch_builds.insert(sketch_dir.clone(), SketchBuildState::Failed("Compile failed".to_string()));
                            for dev in bd.devices.iter_mut() {
                                if dev.sketch_dir == sketch_dir {
                                    dev.phase = DeployPhase::Failed("Compile failed".to_string());
                                }
                            }
                        }
                        self.egui_ctx.request_repaint();
                    }
                }
                AppEvent::BulkUploadDone { sketch_dir, url } => {
                    if let Some(bd) = &mut self.bulk_deploy {
                        bd.sketch_builds.insert(sketch_dir.clone(), SketchBuildState::Uploaded(url.clone()));
                        // OTA trigger all devices that use this sketch
                        let device_ids: Vec<String> = bd.devices.iter()
                            .filter(|d| d.sketch_dir == sketch_dir)
                            .map(|d| d.device_id.clone())
                            .collect();
                        for dev in bd.devices.iter_mut() {
                            if dev.sketch_dir == sketch_dir {
                                dev.phase = DeployPhase::Publishing;
                            }
                        }
                        // Publish OTA trigger for each device
                        if let Some(client) = self.mqtt_client.lock().unwrap().as_ref() {
                            for did in &device_ids {
                                let topic = format!("solar/{}/ota", did);
                                let _ = client.publish(&topic, QoS::AtLeastOnce, false, url.as_bytes());
                            }
                        }
                        for dev in bd.devices.iter_mut() {
                            if dev.sketch_dir == sketch_dir {
                                dev.phase = DeployPhase::Waiting;
                            }
                        }
                        self.egui_ctx.request_repaint();
                    }
                }
                AppEvent::BulkOtaPublished { device_id } => {
                    if let Some(bd) = &mut self.bulk_deploy {
                        for dev in bd.devices.iter_mut() {
                            if dev.device_id == device_id {
                                dev.phase = DeployPhase::Done;
                            }
                        }
                        self.egui_ctx.request_repaint();
                    }
                }
            }
        }
    }

    // ── Visuals ───────────────────────────────────────────────────────────────

    fn setup_visuals(&self, ctx: &egui::Context) {
        let mut v = egui::Visuals::dark();
        v.panel_fill               = BG;
        v.window_fill              = SURFACE;
        v.override_text_color      = Some(TEXT);
        v.extreme_bg_color         = Color32::from_rgb(8, 9, 12);
        v.widgets.noninteractive.bg_stroke = Stroke::new(1.0, BORDER);
        v.widgets.inactive.bg_fill         = SURFACE;
        v.widgets.inactive.bg_stroke       = Stroke::new(1.0, BORDER);
        v.widgets.hovered.bg_fill          = Color32::from_rgb(30, 35, 46);
        v.widgets.hovered.bg_stroke        = Stroke::new(1.0, PRIMARY);
        v.widgets.active.bg_fill           = PRIMARY;
        v.selection.bg_fill                = Color32::from_rgba_premultiplied(79, 142, 247, 40);
        ctx.set_visuals(v);
    }

    // ── Render helpers ────────────────────────────────────────────────────────

    fn card_frame() -> egui::Frame {
        egui::Frame {
            fill: CARD,
            stroke: Stroke::new(1.0, BORDER),
            rounding: Rounding::same(8.0),
            inner_margin: Margin::same(16.0),
            ..Default::default()
        }
    }

    fn label_mono(s: impl Into<String>, color: Color32) -> RichText {
        RichText::new(s).monospace().color(color).size(12.0)
    }

    // ── Top bar ───────────────────────────────────────────────────────────────

    fn render_top_bar(&mut self, ctx: &egui::Context) {
        egui::TopBottomPanel::top("top_bar")
            .frame(egui::Frame { fill: SURFACE, stroke: Stroke::new(1.0, BORDER), inner_margin: Margin::symmetric(16.0, 8.0), ..Default::default() })
            .show(ctx, |ui| {
                ui.horizontal(|ui| {
                    ui.label(RichText::new("OTA Flasher").size(18.0).strong().color(TEXT));
                    ui.add_space(6.0);
                    ui.label(RichText::new("ESP32 Fleet").size(12.0).color(MUTED));

                    ui.with_layout(egui::Layout::right_to_left(egui::Align::Center), |ui| {
                        // Settings
                        if ui.button("⚙  Settings").clicked() {
                            self.show_add = false;
                            self.settings_buf = self.config.clone();
                            self.show_settings = true;
                        }

                        ui.add_space(8.0);

                        // Add device
                        let btn = egui::Button::new(RichText::new("+ Add Device").color(Color32::WHITE))
                            .fill(PRIMARY);
                        if ui.add(btn).clicked() {
                            self.show_settings = false;
                            self.add_form = AddForm {
                                version: "1.0.0".to_string(),
                                device_id: suggest_next_id(&self.fleet.devices),
                                ..AddForm::default()
                            };
                            if !self.add_form.device_id.is_empty() {
                                self.add_form.sketch_dir = default_sketch_dir(
                                    &self.config.sketch_root,
                                    &self.add_form.device_id,
                                );
                            }
                            self.show_add = true;
                        }

                        // Bulk deploy button — visible only when devices are selected
                        if !self.selected.is_empty() {
                            ui.add_space(8.0);
                            let label = format!("Deploy {} devices", self.selected.len());
                            let btn = egui::Button::new(RichText::new(&label).color(Color32::WHITE).size(13.0))
                                .fill(Color32::from_rgb(180, 80, 80));
                            if ui.add(btn).clicked() {
                                self.open_bulk_deploy_form();
                            }
                        }

                        ui.add_space(16.0);

                        // MQTT indicator
                        let (dot, txt, col) = if self.mqtt_connected {
                            ("[+]", "MQTT connected", SUCCESS)
                        } else {
                            ("[ ]", "MQTT offline", MUTED)
                        };
                        ui.colored_label(col, format!("{} {}", dot, txt));

                        ui.add_space(8.0);

                        // Fleet info
                        if self.fleet_loaded {
                            ui.colored_label(MUTED, format!("{} devices", self.fleet.devices.len()));
                        } else {
                            ui.colored_label(MUTED, "Loading…");
                        }
                        ui.add_space(8.0);
                    });
                });
            });
    }

    // ── Notification bar ──────────────────────────────────────────────────────

    fn render_notif(&mut self, ctx: &egui::Context) {
        let show = self.notif.as_ref().map(|n| n.at.elapsed().as_secs() < 5).unwrap_or(false);
        if !show {
            self.notif = None;
            return;
        }
        let (msg, col) = {
            let n = self.notif.as_ref().unwrap();
            (n.msg.clone(), if n.is_err { DANGER } else { SUCCESS })
        };
        egui::TopBottomPanel::top("notif")
            .frame(egui::Frame { fill: col, inner_margin: Margin::symmetric(16.0, 6.0), ..Default::default() })
            .show(ctx, |ui| {
                ui.centered_and_justified(|ui| {
                    ui.label(RichText::new(&msg).color(Color32::WHITE).size(13.0));
                });
            });
        ctx.request_repaint_after(std::time::Duration::from_secs(1));
    }

    // ── Tab bar ───────────────────────────────────────────────────────────────

    fn render_tabs(&mut self, ui: &mut egui::Ui) {
        ui.horizontal(|ui| {
            ui.add_space(4.0);
            for (label, t) in [("Devices", Tab::Devices), ("Deploy History", Tab::History)] {
                let sel = self.tab == t;
                let col = if sel { TEXT } else { MUTED };
                let btn = egui::Button::new(RichText::new(label).color(col).size(13.0))
                    .fill(Color32::TRANSPARENT)
                    .stroke(if sel { Stroke::new(1.0, PRIMARY) } else { Stroke::NONE });
                if ui.add(btn).clicked() {
                    self.tab = t;
                }
                ui.add_space(2.0);
            }
        });
        ui.add_space(4.0);
        ui.separator();
        ui.add_space(10.0);
    }

    // ── Search bar ───────────────────────────────────────────────────────────

    fn render_toolbar(&mut self, ui: &mut egui::Ui) {
        ui.horizontal(|ui| {
            let search = egui::TextEdit::singleline(&mut self.search_query)
                .hint_text("Search by name or ID…")
                .desired_width(260.0);
            ui.add(search);

            if !self.search_query.is_empty() {
                if ui.small_button("✕").clicked() {
                    self.search_query.clear();
                }
            }
        });
        ui.add_space(10.0);
    }

    fn filtered_devices(&self) -> Vec<Device> {
        let q = self.search_query.trim().to_lowercase();
        if q.is_empty() {
            self.fleet.devices.clone()
        } else {
            self.fleet.devices.iter().filter(|d| {
                d.name.to_lowercase().contains(&q)
                    || d.id.to_lowercase().contains(&q)
                    || d.company.to_lowercase().contains(&q)
                    || d.tags.iter().any(|t| t.to_lowercase().contains(&q))
            }).cloned().collect()
        }
    }

    // ── Device grid ───────────────────────────────────────────────────────────

    fn render_devices(&mut self, ui: &mut egui::Ui) {
        self.render_toolbar(ui);

        let devices = self.filtered_devices();

        if devices.is_empty() {
            ui.add_space(60.0);
            ui.vertical_centered(|ui| {
                if self.fleet.devices.is_empty() {
                    ui.colored_label(MUTED, RichText::new("No devices yet").size(16.0));
                    ui.add_space(8.0);
                    ui.colored_label(MUTED, "Click  + Add Device  to register your first ESP32.");
                } else {
                    ui.colored_label(MUTED, RichText::new("No devices match your search").size(16.0));
                }
            });
            return;
        }

        self.render_devices_list(ui, &devices);
    }

    fn render_devices_list(&mut self, ui: &mut egui::Ui, devices: &[Device]) {
        // Select-all / clear controls
        let all_ids: HashSet<String> = devices.iter().map(|d| d.id.clone()).collect();
        let all_selected = !all_ids.is_empty() && all_ids.iter().all(|id| self.selected.contains(id));

        ui.horizontal(|ui| {
            let mut all = all_selected;
            if ui.checkbox(&mut all, "").changed() {
                if all {
                    for id in &all_ids { self.selected.insert(id.clone()); }
                } else {
                    for id in &all_ids { self.selected.remove(id); }
                }
            }
            ui.label(RichText::new("Select all visible").size(12.0).color(MUTED));
            if !self.selected.is_empty() {
                ui.add_space(8.0);
                if ui.small_button("Clear selection").clicked() {
                    self.selected.clear();
                }
                ui.add_space(4.0);
                ui.label(RichText::new(format!("{} selected", self.selected.len())).size(12.0).color(PRIMARY));
            }
        });
        ui.add_space(4.0);

        // Header row
        egui::Frame {
            fill: SURFACE,
            stroke: Stroke::new(1.0, BORDER),
            rounding: Rounding::same(4.0),
            inner_margin: Margin::symmetric(10.0, 6.0),
            ..Default::default()
        }.show(ui, |ui| {
            ui.set_min_width(ui.available_width());
            ui.horizontal(|ui| {
                ui.add_space(24.0); // checkbox column width
                ui.allocate_ui(egui::vec2(200.0, 0.0), |ui| {
                    ui.label(RichText::new("Name").size(11.0).color(MUTED).strong());
                });
                ui.allocate_ui(egui::vec2(110.0, 0.0), |ui| {
                    ui.label(RichText::new("ID").size(11.0).color(MUTED).strong());
                });
                ui.allocate_ui(egui::vec2(100.0, 0.0), |ui| {
                    ui.label(RichText::new("Company").size(11.0).color(MUTED).strong());
                });
                ui.allocate_ui(egui::vec2(70.0, 0.0), |ui| {
                    ui.label(RichText::new("Status").size(11.0).color(MUTED).strong());
                });
                ui.allocate_ui(egui::vec2(90.0, 0.0), |ui| {
                    ui.label(RichText::new("Deployed").size(11.0).color(MUTED).strong());
                });
                ui.allocate_ui(egui::vec2(90.0, 0.0), |ui| {
                    ui.label(RichText::new("Running").size(11.0).color(MUTED).strong());
                });
            });
        });
        ui.add_space(2.0);

        let devices_cloned: Vec<Device> = devices.to_vec();
        egui::ScrollArea::vertical().show(ui, |ui| {
            for (row_idx, device) in devices_cloned.iter().enumerate() {
                let raw_status = self.mqtt_status.get(&device.id).cloned().unwrap_or_default();
                let is_online  = raw_status.starts_with("ONLINE");
                let reported   = parse_version_from_status(&raw_status);

                let row_bg = if row_idx % 2 == 0 { CARD } else { Color32::from_rgb(20, 23, 30) };
                egui::Frame {
                    fill: row_bg,
                    stroke: Stroke::new(1.0, BORDER),
                    rounding: Rounding::same(4.0),
                    inner_margin: Margin::symmetric(10.0, 5.0),
                    ..Default::default()
                }.show(ui, |ui| {
                    ui.set_min_width(ui.available_width());
                    ui.horizontal(|ui| {
                        // Checkbox
                        let mut checked = self.selected.contains(&device.id);
                        if ui.checkbox(&mut checked, "").changed() {
                            if checked {
                                self.selected.insert(device.id.clone());
                            } else {
                                self.selected.remove(&device.id);
                            }
                        }
                        // Name + tags
                        ui.allocate_ui(egui::vec2(200.0, 0.0), |ui| {
                            ui.vertical(|ui| {
                                ui.label(RichText::new(&device.name).size(13.0).color(TEXT));
                                if !device.tags.is_empty() {
                                    ui.horizontal_wrapped(|ui| {
                                        ui.spacing_mut().item_spacing.x = 4.0;
                                        for tag in &device.tags {
                                            egui::Frame {
                                                fill: Color32::from_rgb(40, 60, 100),
                                                rounding: Rounding::same(3.0),
                                                inner_margin: Margin::symmetric(4.0, 1.0),
                                                ..Default::default()
                                            }.show(ui, |ui| {
                                                ui.label(RichText::new(tag).size(10.0).color(PRIMARY));
                                            });
                                        }
                                    });
                                }
                            });
                        });
                        // ID
                        ui.allocate_ui(egui::vec2(110.0, 0.0), |ui| {
                            ui.label(Self::label_mono(&device.id, MUTED));
                        });
                        // Company
                        ui.allocate_ui(egui::vec2(100.0, 0.0), |ui| {
                            if !device.company.is_empty() {
                                ui.label(RichText::new(&device.company).size(12.0).color(MUTED));
                            }
                        });
                        // Status
                        ui.allocate_ui(egui::vec2(70.0, 0.0), |ui| {
                            let (dot, col, label) = if is_online {
                                ("●", SUCCESS, "Online")
                            } else {
                                ("○", MUTED, "Offline")
                            };
                            ui.label(RichText::new(format!("{} {}", dot, label)).size(12.0).color(col));
                        });
                        // Deployed version
                        ui.allocate_ui(egui::vec2(90.0, 0.0), |ui| {
                            let deployed = if device.desired_version.is_empty() {
                                "—".to_string()
                            } else {
                                format!("v{}", device.desired_version)
                            };
                            ui.label(Self::label_mono(deployed, TEXT));
                        });
                        // Running version
                        ui.allocate_ui(egui::vec2(90.0, 0.0), |ui| {
                            if let Some(rv) = &reported {
                                let same = rv == &device.desired_version;
                                let (col, mark) = if same { (SUCCESS, "✓") } else { (WARNING, "⚠") };
                                ui.label(RichText::new(format!("v{} {}", rv, mark)).size(12.0).color(col));
                            } else {
                                ui.label(Self::label_mono("—", MUTED));
                            }
                        });
                        // Deploy button — right-aligned
                        ui.with_layout(egui::Layout::right_to_left(egui::Align::Center), |ui| {
                            let btn = egui::Button::new(
                                RichText::new("Deploy").size(12.0).color(Color32::WHITE)
                            ).fill(PRIMARY).min_size(egui::vec2(60.0, 22.0));
                            if ui.add(btn).clicked() {
                                let dev = device.clone();
                                self.open_deploy(&dev);
                            }
                        });
                    });
                });
                ui.add_space(2.0);
            }
        });
    }

    // ── History tab ───────────────────────────────────────────────────────────

    fn render_history(&self, ui: &mut egui::Ui) {
        if self.fleet.deploy_history.is_empty() {
            ui.add_space(60.0);
            ui.vertical_centered(|ui| {
                ui.colored_label(MUTED, RichText::new("No deploy history yet.").size(16.0));
            });
            return;
        }

        egui::ScrollArea::vertical().show(ui, |ui| {
            let mut history = self.fleet.deploy_history.clone();
            history.reverse();
            for rec in &history {
                Self::card_frame().show(ui, |ui| {
                    ui.set_width(ui.available_width());
                    ui.horizontal(|ui| {
                        ui.vertical(|ui| {
                            ui.label(RichText::new(&rec.device_name).size(14.0).strong().color(TEXT));
                            ui.label(Self::label_mono(format!("solar/{}/…", rec.device_id), MUTED));
                        });
                        ui.with_layout(egui::Layout::right_to_left(egui::Align::TOP), |ui| {
                            ui.label(RichText::new(&rec.deployed_at).size(11.0).color(MUTED));
                            ui.add_space(8.0);
                            ui.colored_label(SUCCESS, RichText::new(format!("v{}", rec.version)).monospace().size(13.0));
                        });
                    });
                    ui.add_space(4.0);
                    ui.label(RichText::new(format!("by {}", rec.deployed_by)).size(12.0).color(MUTED));
                });
                ui.add_space(6.0);
            }
        });
    }

    // ── Deploy window ─────────────────────────────────────────────────────────

    fn render_deploy_window(&mut self, ctx: &egui::Context) {
        if self.deploy.is_none() { return; }

        let title = self.deploy.as_ref().map(|d| format!("Deploy → {}", d.device_name)).unwrap_or_default();
        let mut open = true;

        egui::Window::new(title)
            .id(egui::Id::new("deploy_win"))
            .default_size([580.0, 520.0])
            .min_size([480.0, 400.0])
            .collapsible(false)
            .open(&mut open)
            .show(ctx, |ui| {
                self.render_deploy_inner(ui, ctx);
            });

        if !open { self.deploy = None; }
    }

    fn render_deploy_inner(&mut self, ui: &mut egui::Ui, ctx: &egui::Context) {
        let phase = match self.deploy.as_ref() {
            Some(d) => d.phase.clone(),
            None => return,
        };

        // ── Step indicators
        ui.horizontal(|ui| {
            let steps: &[(&str, &[DeployPhase], &[DeployPhase])] = &[
                ("Compile", &[DeployPhase::Compiling], &[DeployPhase::Uploading, DeployPhase::Publishing, DeployPhase::Waiting, DeployPhase::Done]),
                ("Upload",  &[DeployPhase::Uploading],  &[DeployPhase::Publishing, DeployPhase::Waiting, DeployPhase::Done]),
                ("OTA",     &[DeployPhase::Publishing], &[DeployPhase::Waiting, DeployPhase::Done]),
                ("Confirm", &[DeployPhase::Waiting],    &[DeployPhase::Done]),
            ];
            for (i, (label, active, done)) in steps.iter().enumerate() {
                if i > 0 { ui.colored_label(BORDER, " › "); }
                let is_active = active.contains(&phase);
                let is_done   = done.contains(&phase);
                let col    = if is_done { SUCCESS } else if is_active { PRIMARY } else { MUTED };
                let prefix = if is_done { "✓ " } else if is_active { "● " } else { "○ " };
                ui.colored_label(col, format!("{}{}", prefix, label));
            }
            if let DeployPhase::Failed(ref e) = phase {
                ui.colored_label(DANGER, format!("  ✗ {}", e));
            }
        });
        ui.add_space(8.0);
        ui.separator();
        ui.add_space(10.0);

        // Flags set inside closures, actions executed after rendering
        let mut pick_sketch = false;
        let mut do_start    = false;
        let mut do_close    = false;

        // ── Form fields
        if phase == DeployPhase::Form || matches!(phase, DeployPhase::Failed(_)) {
            // Clone data out → render into locals → write back after closures
            let (mut dname, mut dver, mut dsketch) = {
                let d = self.deploy.as_ref().unwrap();
                (d.deployer_name.clone(), d.new_version.clone(), d.sketch_dir.clone())
            };

            egui::Grid::new("deploy_grid")
                .num_columns(2)
                .spacing([12.0, 8.0])
                .show(ui, |ui| {
                    ui.label(RichText::new("Your name").color(MUTED));
                    ui.add(egui::TextEdit::singleline(&mut dname).hint_text("Srikar").desired_width(260.0));
                    ui.end_row();

                    ui.label(RichText::new("New version").color(MUTED));
                    ui.add(egui::TextEdit::singleline(&mut dver)
                        .hint_text("1.0.2").desired_width(160.0).font(FontId::monospace(13.0)));
                    ui.end_row();

                    ui.label(RichText::new("Sketch folder").color(MUTED));
                    ui.horizontal(|ui| {
                        ui.add(egui::TextEdit::singleline(&mut dsketch)
                            .desired_width(200.0).font(FontId::new(11.0, FontFamily::Monospace)));
                        if ui.button("Browse").clicked() { pick_sketch = true; }
                    });
                    ui.end_row();
                });

            // Write back
            if let Some(d) = &mut self.deploy {
                d.deployer_name = dname;
                d.new_version   = dver;
                d.sketch_dir    = dsketch;
            }

            let ready = self.deploy.as_ref().map(|d| !d.deployer_name.trim().is_empty() && !d.new_version.trim().is_empty()).unwrap_or(false);
            ui.add_space(10.0);
            ui.with_layout(egui::Layout::right_to_left(egui::Align::TOP), |ui| {
                let btn = egui::Button::new(RichText::new("▶  Build & Deploy").color(Color32::WHITE))
                    .fill(if ready { PRIMARY } else { Color32::from_rgb(50, 50, 60) });
                if ui.add_enabled(ready, btn).clicked() { do_start = true; }
            });
        } else {
            // Compact status during active deploy
            let (ver, dname, deployer) = {
                let d = self.deploy.as_ref().unwrap();
                (d.new_version.clone(), d.device_name.clone(), d.deployer_name.clone())
            };
            ui.horizontal(|ui| {
                ui.colored_label(MUTED, "Deploying:");
                ui.colored_label(TEXT, format!("v{}  →  {}", ver, dname));
                ui.with_layout(egui::Layout::right_to_left(egui::Align::Center), |ui| {
                    ui.colored_label(MUTED, format!("by {}", deployer));
                });
            });
            ui.add_space(6.0);

            if phase == DeployPhase::Waiting {
                let t = ctx.input(|i| i.time) as f32;
                let frames = ["⠋","⠙","⠹","⠸","⠼","⠴","⠦","⠧","⠇","⠏"];
                let idx = ((t * 8.0) as usize) % frames.len();
                ui.colored_label(WARNING, format!("{} Waiting for device to confirm v{}…", frames[idx], ver));
                ctx.request_repaint_after(std::time::Duration::from_millis(120));
            }
            if phase == DeployPhase::Done {
                ui.colored_label(SUCCESS, format!("✓ Deploy complete! Device is running v{}", ver));
            }
        }

        // ── Compile log
        let log_lines: Vec<(String, LogLevel)> = self.deploy.as_ref()
            .map(|d| d.log_lines.clone())
            .unwrap_or_default();

        if !log_lines.is_empty() {
            ui.add_space(8.0);
            ui.separator();
            ui.add_space(4.0);
            ui.label(RichText::new("Output").size(11.0).color(MUTED));
            ui.add_space(4.0);

            let log_h = if phase == DeployPhase::Form { 150.0 } else { 220.0 };
            egui::Frame {
                fill: Color32::from_rgb(8, 9, 12),
                stroke: Stroke::new(1.0, BORDER),
                rounding: Rounding::same(6.0),
                inner_margin: Margin::same(10.0),
                ..Default::default()
            }
            .show(ui, |ui| {
                egui::ScrollArea::vertical()
                    .id_salt("compile_log")
                    .max_height(log_h)
                    .stick_to_bottom(true)
                    .show(ui, |ui| {
                        for (line, level) in &log_lines {
                            let col = match level {
                                LogLevel::Ok     => SUCCESS,
                                LogLevel::Error  => DANGER,
                                LogLevel::Info   => PRIMARY,
                                LogLevel::Normal => Color32::from_rgb(155, 162, 175),
                            };
                            ui.label(RichText::new(line).monospace().size(11.0).color(col));
                        }
                    });
            });
        }

        // ── Done/Failed: close button
        if matches!(phase, DeployPhase::Done | DeployPhase::Failed(_)) {
            ui.add_space(8.0);
            ui.with_layout(egui::Layout::right_to_left(egui::Align::TOP), |ui| {
                if ui.button("Close").clicked() { do_close = true; }
            });
        }

        // Execute deferred actions
        if pick_sketch  { self.pick_folder(FolderPickCtx::DeploySketch); }
        if do_start     { self.start_deploy(); }
        if do_close     { self.deploy = None; }
    }

    // ── Add device window ─────────────────────────────────────────────────────

    fn render_add_window(&mut self, ctx: &egui::Context) {
        if !self.show_add { return; }

        let mut open    = self.show_add;
        let mut pick_f  = false;
        let mut do_save = false;
        let mut cancel  = false;

        // Clone form data to avoid borrowing self inside closures
        let mut by_name     = self.add_form.by_name.clone();
        let mut company     = self.add_form.company.clone();
        let mut device_name = self.add_form.device_name.clone();
        let mut device_id   = self.add_form.device_id.clone();
        let mut version     = self.add_form.version.clone();
        let mut sketch_dir  = self.add_form.sketch_dir.clone();
        let mut tags_input  = self.add_form.tags_input.clone();
        let error           = self.add_form.error.clone();

        egui::Window::new("Add Device")
            .id(egui::Id::new("add_win"))
            .default_size([520.0, 430.0])
            .collapsible(false)
            .open(&mut open)
            .show(ctx, |ui| {
                egui::Grid::new("add_grid")
                    .num_columns(2)
                    .spacing([12.0, 8.0])
                    .show(ui, |ui| {
                        ui.label(RichText::new("Your name").color(MUTED));
                        ui.add(egui::TextEdit::singleline(&mut by_name).hint_text("Srikar").desired_width(240.0));
                        ui.end_row();

                        ui.label(RichText::new("Company").color(MUTED));
                        ui.add(egui::TextEdit::singleline(&mut company).hint_text("Aditya").desired_width(240.0));
                        ui.end_row();

                        ui.label(RichText::new("Device name").color(MUTED));
                        ui.add(egui::TextEdit::singleline(&mut device_name).hint_text("e.g. 'elmeasure'").desired_width(240.0));
                        ui.end_row();

                        ui.label(RichText::new("MQTT topic ID").color(MUTED));
                        ui.add(egui::TextEdit::singleline(&mut device_id)
                            .hint_text("e.g. 'vit010'").font(FontId::monospace(13.0)).desired_width(200.0));
                        ui.end_row();

                        ui.label(RichText::new("Initial version").color(MUTED));
                        ui.add(egui::TextEdit::singleline(&mut version)
                            .font(FontId::monospace(13.0)).desired_width(120.0));
                        ui.end_row();

                        ui.label(RichText::new("Sketch folder").color(MUTED));
                        ui.horizontal(|ui| {
                            ui.add(egui::TextEdit::singleline(&mut sketch_dir)
                                .desired_width(180.0).font(FontId::new(11.0, FontFamily::Monospace)));
                            if ui.button("Browse").clicked() { pick_f = true; }
                        });
                        ui.end_row();

                        ui.label(RichText::new("Tags").color(MUTED));
                        ui.add(egui::TextEdit::singleline(&mut tags_input)
                            .hint_text("RS485, Floor 1 (comma-separated)")
                            .desired_width(240.0));
                        ui.end_row();
                    });

                if !device_id.is_empty() {
                    ui.add_space(8.0);
                    ui.separator();
                    ui.add_space(6.0);
                    ui.label(RichText::new("MQTT topics that will be created:").size(11.0).color(MUTED));
                    ui.add_space(4.0);
                    for suffix in ["cmd", "status", "ota"] {
                        ui.label(Self::label_mono(format!("solar/{}/{}", device_id, suffix), MUTED));
                    }
                }

                if !error.is_empty() {
                    ui.add_space(6.0);
                    ui.colored_label(DANGER, &error);
                }

                ui.add_space(12.0);
                ui.with_layout(egui::Layout::right_to_left(egui::Align::TOP), |ui| {
                    let btn = egui::Button::new(RichText::new("Save Device").color(Color32::WHITE)).fill(PRIMARY);
                    if ui.add(btn).clicked() { do_save = true; }
                    ui.add_space(8.0);
                    if ui.button("Cancel").clicked() { cancel = true; }
                });
            });

        // Write back form data
        self.add_form.by_name     = by_name;
        self.add_form.company     = company;
        self.add_form.device_name = device_name;
        self.add_form.device_id   = device_id;
        self.add_form.version     = version;
        self.add_form.sketch_dir  = sketch_dir;
        self.add_form.tags_input  = tags_input;

        self.show_add = open;

        if pick_f   { self.pick_folder(FolderPickCtx::AddDeviceSketch); }
        if do_save  { self.do_add_device(); }
        if cancel   { self.show_add = false; }
    }

    fn do_add_device(&mut self) {
        let f = &self.add_form;
        if f.by_name.trim().is_empty() || f.device_name.trim().is_empty() || f.device_id.trim().is_empty() || f.version.trim().is_empty() {
            self.add_form.error = "All fields (except company) are required.".to_string();
            return;
        }
        if self.fleet.devices.iter().any(|d| d.id == f.device_id) {
            self.add_form.error = format!("Device '{}' already exists.", f.device_id);
            return;
        }

        let id = f.device_id.trim().to_string();
        let device = Device {
            id: id.clone(),
            name: f.device_name.trim().to_string(),
            company: f.company.trim().to_string(),
            sketch_dir: f.sketch_dir.trim().to_string(),
            topics: crate::types::Topics {
                cmd:    format!("solar/{}/cmd",    id),
                status: format!("solar/{}/status", id),
                ota:    format!("solar/{}/ota",    id),
            },
            desired_version: f.version.trim().to_string(),
            last_deploy_by: String::new(),
            added_by: f.by_name.trim().to_string(),
            added_at: chrono::Utc::now().to_rfc3339(),
            tags: f.tags_input.split(',')
                .map(|s| s.trim().to_string())
                .filter(|s| !s.is_empty())
                .collect(),
        };

        self.fleet.devices.push(device);
        self.save_fleet();
        self.notify(format!("Device '{}' added.", id), false);
        self.show_add = false;
        self.add_form = AddForm::default();
    }

    // ── Settings window ───────────────────────────────────────────────────────

    fn render_settings_window(&mut self, ctx: &egui::Context) {
        if !self.show_settings { return; }

        let first_run = self.first_run;
        let mut open     = if first_run { true } else { self.show_settings };
        let mut pick_root = false;
        let mut do_save  = false;
        let mut cancel   = false;

        let mut s = self.settings_buf.clone();

        let missing_url   = s.worker_url.trim().is_empty();
        let missing_token = s.worker_token.trim().is_empty();
        let missing_pass  = s.mqtt_pass.trim().is_empty();
        let can_save      = !missing_url && !missing_token && !missing_pass;

        let title = if first_run { "First-Time Setup" } else { "Settings" };

        let mut win = egui::Window::new(title)
            .id(egui::Id::new("settings_win"))
            .default_size([500.0, 480.0])
            .collapsible(false)
            .resizable(false);

        if !first_run {
            win = win.open(&mut open);
        }

        win.show(ctx, |ui| {
            egui::ScrollArea::vertical().show(ui, |ui| {
                if first_run {
                    egui::Frame::default()
                        .fill(Color32::from_rgb(30, 58, 100))
                        .inner_margin(egui::Margin::symmetric(12.0, 8.0))
                        .rounding(Rounding::same(6.0))
                        .show(ui, |ui| {
                            ui.label(RichText::new("Welcome to OTA Flasher! Fill in the required fields below to get started.").color(Color32::WHITE).size(12.5));
                        });
                    ui.add_space(10.0);
                }

                section_header(ui, "Cloudflare Worker");
                egui::Grid::new("cf_grid").num_columns(2).spacing([10.0, 7.0]).show(ui, |ui| {
                    let url_label = if missing_url {
                        RichText::new("Worker URL *").color(DANGER)
                    } else {
                        RichText::new("Worker URL *").color(MUTED)
                    };
                    ui.label(url_label);
                    ui.add(egui::TextEdit::singleline(&mut s.worker_url)
                        .hint_text("https://ota-api.xxx.workers.dev").desired_width(310.0));
                    ui.end_row();
                    let tok_label = if missing_token {
                        RichText::new("API Token *").color(DANGER)
                    } else {
                        RichText::new("API Token *").color(MUTED)
                    };
                    ui.label(tok_label);
                    ui.add(egui::TextEdit::singleline(&mut s.worker_token)
                        .password(true).desired_width(310.0));
                    ui.end_row();
                });

                ui.add_space(10.0);
                section_header(ui, "Arduino");
                egui::Grid::new("ard_grid").num_columns(2).spacing([10.0, 7.0]).show(ui, |ui| {
                    ui.label(RichText::new("FQBN").color(MUTED));
                    ui.add(egui::TextEdit::singleline(&mut s.fqbn)
                        .font(FontId::monospace(12.0)).desired_width(240.0));
                    ui.end_row();
                    ui.label(RichText::new("Sketches root").color(MUTED));
                    ui.horizontal(|ui| {
                        ui.add(egui::TextEdit::singleline(&mut s.sketch_root)
                            .font(FontId::new(11.0, FontFamily::Monospace)).desired_width(210.0));
                        if ui.button("Browse").clicked() { pick_root = true; }
                    });
                    ui.end_row();
                });

                ui.add_space(10.0);
                section_header(ui, "MQTT Broker");
                egui::Grid::new("mqtt_grid").num_columns(2).spacing([10.0, 7.0]).show(ui, |ui| {
                    ui.label(RichText::new("Host").color(MUTED));
                    ui.add(egui::TextEdit::singleline(&mut s.mqtt_host).desired_width(240.0));
                    ui.end_row();
                    ui.label(RichText::new("Port").color(MUTED));
                    let mut port_s = s.mqtt_port.to_string();
                    if ui.add(egui::TextEdit::singleline(&mut port_s).desired_width(80.0)).changed() {
                        if let Ok(p) = port_s.parse::<u16>() { s.mqtt_port = p; }
                    }
                    ui.end_row();
                    ui.label(RichText::new("User").color(MUTED));
                    ui.add(egui::TextEdit::singleline(&mut s.mqtt_user).desired_width(180.0));
                    ui.end_row();
                    let pass_label = if missing_pass {
                        RichText::new("Password *").color(DANGER)
                    } else {
                        RichText::new("Password *").color(MUTED)
                    };
                    ui.label(pass_label);
                    ui.add(egui::TextEdit::singleline(&mut s.mqtt_pass).password(true).desired_width(180.0));
                    ui.end_row();
                });

                ui.add_space(10.0);
                section_header(ui, "VPS Firmware Delivery");
                ui.colored_label(MUTED, RichText::new("Firmware is served over plain HTTP from your VPS — avoids ESP32 TLS timeout on large binaries.").size(11.0));
                ui.add_space(6.0);
                egui::Grid::new("vps_grid").num_columns(2).spacing([10.0, 7.0]).show(ui, |ui| {
                    ui.label(RichText::new("VPS host").color(MUTED));
                    ui.add(egui::TextEdit::singleline(&mut s.vps_host)
                        .font(FontId::monospace(12.0)).desired_width(240.0)
                        .hint_text("root@1.2.3.4"));
                    ui.end_row();
                    ui.label(RichText::new("SSH key path").color(MUTED));
                    ui.add(egui::TextEdit::singleline(&mut s.vps_key)
                        .font(FontId::new(11.0, FontFamily::Monospace)).desired_width(240.0)
                        .hint_text("~/.ssh/id_rsa"));
                    ui.end_row();
                    ui.label(RichText::new("Firmware dir").color(MUTED));
                    ui.add(egui::TextEdit::singleline(&mut s.vps_firmware_dir)
                        .font(FontId::monospace(12.0)).desired_width(240.0)
                        .hint_text("/var/www/firmware"));
                    ui.end_row();
                    ui.label(RichText::new("Firmware URL base").color(MUTED));
                    ui.add(egui::TextEdit::singleline(&mut s.vps_firmware_url)
                        .font(FontId::monospace(12.0)).desired_width(240.0)
                        .hint_text("http://your-vps/firmware"));
                    ui.end_row();
                });

                if first_run && !can_save {
                    ui.add_space(8.0);
                    ui.label(RichText::new("* Required fields must be filled before you can continue.").color(DANGER).size(11.0));
                }

                ui.add_space(14.0);
                ui.with_layout(egui::Layout::right_to_left(egui::Align::TOP), |ui| {
                    let save_btn = egui::Button::new(RichText::new("Save & Apply").color(Color32::WHITE)).fill(
                        if can_save { PRIMARY } else { Color32::from_rgb(60, 60, 70) }
                    );
                    if ui.add_enabled(can_save, save_btn).clicked() { do_save = true; }
                    if !first_run {
                        ui.add_space(8.0);
                        if ui.button("Cancel").clicked() { cancel = true; }
                    }
                });
            });
        });

        self.settings_buf = s;
        if !first_run { self.show_settings = open; }

        if pick_root { self.pick_folder(FolderPickCtx::SettingsSketchRoot); }
        if cancel    { self.show_settings = false; }
        if do_save {
            self.config = self.settings_buf.clone();
            save_config(&self.config);
            self.show_settings = false;
            self.first_run = false;
            if !self.config.worker_url.is_empty() { self.load_fleet(); }
            self.notify("Settings saved. Reconnecting MQTT…", false);
            *self.mqtt_client.lock().unwrap() = None;
            self.mqtt_connected = false;
            self.mqtt_gen.fetch_add(1, Ordering::SeqCst); // cancel pending reconnect threads
            let cfg  = self.config.clone();
            let arc  = self.mqtt_client.clone();
            let tx   = self.event_tx.clone();
            let ctx2 = self.egui_ctx.clone();
            std::thread::spawn(move || run_mqtt(cfg, arc, tx, ctx2));
        }
    }
}

// ── eframe::App ───────────────────────────────────────────────────────────────

impl eframe::App for App {
    fn update(&mut self, ctx: &egui::Context, _frame: &mut eframe::Frame) {
        self.handle_events();
        self.setup_visuals(ctx);
        self.render_top_bar(ctx);
        self.render_notif(ctx);

        egui::CentralPanel::default()
            .frame(egui::Frame { fill: BG, inner_margin: Margin::same(16.0), ..Default::default() })
            .show(ctx, |ui| {
                self.render_tabs(ui);
                egui::ScrollArea::vertical().show(ui, |ui| {
                    match self.tab {
                        Tab::Devices => self.render_devices(ui),
                        Tab::History => self.render_history(ui),
                    }
                });
            });

        self.render_deploy_window(ctx);
        self.render_add_window(ctx);
        self.render_settings_window(ctx);
        self.render_bulk_deploy_window(ctx);
    }
}

impl App {
    // ── Bulk deploy window ────────────────────────────────────────────────────

    fn render_bulk_deploy_window(&mut self, ctx: &egui::Context) {
        if self.bulk_deploy.is_none() { return; }

        let bd = self.bulk_deploy.as_ref().unwrap();
        let show_form = bd.show_form;
        let n = bd.devices.len();

        if show_form {
            // ── Form: enter deployer name + version before starting
            let mut deployer = bd.form_deployer.clone();
            let mut version  = bd.form_version.clone();
            let mut do_start = false;
            let mut do_cancel = false;

            egui::Window::new(format!("Bulk Deploy — {} devices", n))
                .id(egui::Id::new("bulk_form_win"))
                .default_size([400.0, 200.0])
                .collapsible(false)
                .show(ctx, |ui| {
                    egui::Grid::new("bulk_form_grid")
                        .num_columns(2)
                        .spacing([12.0, 8.0])
                        .show(ui, |ui| {
                            ui.label(RichText::new("Your name").color(MUTED));
                            ui.add(egui::TextEdit::singleline(&mut deployer)
                                .hint_text("Srikar").desired_width(220.0));
                            ui.end_row();

                            ui.label(RichText::new("New version").color(MUTED));
                            ui.add(egui::TextEdit::singleline(&mut version)
                                .font(FontId::monospace(13.0)).desired_width(120.0));
                            ui.end_row();
                        });

                    ui.add_space(8.0);
                    ui.colored_label(MUTED, RichText::new(format!(
                        "Will compile {} sketch(es) and OTA {} device(s).",
                        {
                            let mut dirs = std::collections::HashSet::new();
                            for d in &self.bulk_deploy.as_ref().unwrap().devices {
                                dirs.insert(d.sketch_dir.clone());
                            }
                            dirs.len()
                        },
                        n
                    )).size(11.0));

                    ui.add_space(12.0);
                    ui.with_layout(egui::Layout::right_to_left(egui::Align::TOP), |ui| {
                        let ready = !deployer.trim().is_empty() && !version.trim().is_empty();
                        let btn = egui::Button::new(RichText::new("Start Deploy").color(Color32::WHITE))
                            .fill(if ready { PRIMARY } else { MUTED });
                        if ui.add_enabled(ready, btn).clicked() { do_start = true; }
                        ui.add_space(8.0);
                        if ui.button("Cancel").clicked() { do_cancel = true; }
                    });
                });

            if let Some(bd) = &mut self.bulk_deploy {
                bd.form_deployer = deployer.clone();
                bd.form_version  = version.clone();
            }
            if do_start {
                if let Some(bd) = &mut self.bulk_deploy {
                    bd.deployer_name = bd.form_deployer.trim().to_string();
                    bd.new_version   = bd.form_version.trim().to_string();
                }
                self.start_bulk_deploy();
            }
            if do_cancel {
                self.bulk_deploy = None;
            }
            return;
        }

        // ── Progress view
        let bd = self.bulk_deploy.as_ref().unwrap();
        let devices_snap: Vec<DeviceBulkDeploy> = bd.devices.clone();
        let done_count = devices_snap.iter().filter(|d| d.phase == DeployPhase::Done).count();
        let fail_count = devices_snap.iter().filter(|d| matches!(d.phase, DeployPhase::Failed(_))).count();
        let all_finished = done_count + fail_count == n;

        let mut close = false;
        egui::Window::new(format!("Bulk Deploy — {} devices", n))
            .id(egui::Id::new("bulk_progress_win"))
            .default_size([560.0, 400.0])
            .collapsible(false)
            .show(ctx, |ui| {
                // Summary bar
                egui::Frame {
                    fill: SURFACE,
                    rounding: Rounding::same(4.0),
                    inner_margin: Margin::symmetric(10.0, 6.0),
                    ..Default::default()
                }.show(ui, |ui| {
                    ui.horizontal(|ui| {
                        ui.label(RichText::new(format!("Done: {}", done_count)).color(SUCCESS).size(13.0));
                        ui.add_space(12.0);
                        let in_progress = n - done_count - fail_count;
                        ui.label(RichText::new(format!("In progress: {}", in_progress)).color(PRIMARY).size(13.0));
                        ui.add_space(12.0);
                        ui.label(RichText::new(format!("Failed: {}", fail_count)).color(DANGER).size(13.0));
                    });
                });
                ui.add_space(8.0);

                egui::ScrollArea::vertical().max_height(300.0).show(ui, |ui| {
                    for dev in &devices_snap {
                        let (phase_label, phase_color) = match &dev.phase {
                            DeployPhase::Form        => ("Queued",     MUTED),
                            DeployPhase::Compiling   => ("Compiling…", WARNING),
                            DeployPhase::Uploading   => ("Uploading…", PRIMARY),
                            DeployPhase::Publishing  => ("Sending OTA…", PRIMARY),
                            DeployPhase::Waiting     => ("Waiting…",   PRIMARY),
                            DeployPhase::Done        => ("Done ✓",     SUCCESS),
                            DeployPhase::Failed(_)   => ("Failed ✗",   DANGER),
                        };
                        let progress = match &dev.phase {
                            DeployPhase::Form        => 0.0f32,
                            DeployPhase::Compiling   => 0.20,
                            DeployPhase::Uploading   => 0.45,
                            DeployPhase::Publishing  => 0.70,
                            DeployPhase::Waiting     => 0.85,
                            DeployPhase::Done        => 1.0,
                            DeployPhase::Failed(_)   => 0.0,
                        };

                        egui::Frame {
                            fill: CARD,
                            stroke: Stroke::new(1.0, BORDER),
                            rounding: Rounding::same(4.0),
                            inner_margin: Margin::symmetric(10.0, 6.0),
                            ..Default::default()
                        }.show(ui, |ui| {
                            ui.set_min_width(ui.available_width());
                            ui.horizontal(|ui| {
                                ui.allocate_ui(egui::vec2(160.0, 0.0), |ui| {
                                    ui.label(RichText::new(&dev.device_name).size(13.0).color(TEXT));
                                    ui.label(Self::label_mono(&dev.device_id, MUTED));
                                });
                                ui.allocate_ui(egui::vec2(180.0, 20.0), |ui| {
                                    let bar_color = if matches!(dev.phase, DeployPhase::Failed(_)) { DANGER } else { phase_color };
                                    let pb = egui::ProgressBar::new(progress)
                                        .fill(bar_color)
                                        .desired_width(175.0);
                                    ui.add(pb);
                                });
                                ui.add_space(8.0);
                                ui.label(RichText::new(phase_label).size(12.0).color(phase_color));
                            });
                        });
                        ui.add_space(3.0);
                    }
                });

                ui.add_space(8.0);
                ui.with_layout(egui::Layout::right_to_left(egui::Align::TOP), |ui| {
                    let btn = egui::Button::new(RichText::new("Close").color(Color32::WHITE))
                        .fill(if all_finished { PRIMARY } else { MUTED });
                    if ui.add_enabled(all_finished, btn).clicked() { close = true; }
                    if !all_finished {
                        ui.add_space(8.0);
                        ui.colored_label(MUTED, RichText::new("Close available when all devices finish").size(11.0));
                    }
                });
            });

        if close {
            self.bulk_deploy = None;
            self.selected.clear();
        }

        if !all_finished {
            ctx.request_repaint_after(std::time::Duration::from_millis(300));
        }
    }
}

// ── Pure helpers ──────────────────────────────────────────────────────────────

fn section_header(ui: &mut egui::Ui, title: &str) {
    ui.label(RichText::new(title).size(12.0).strong().color(MUTED));
    ui.separator();
    ui.add_space(4.0);
}

fn parse_version_from_status(status: &str) -> Option<String> {
    // "ONLINE v1.0.1"  →  "1.0.1"
    if !status.starts_with("ONLINE v") { return None; }
    Some(status["ONLINE v".len()..].split_whitespace().next()?.to_string())
}

fn bump_patch(ver: &str) -> String {
    let parts: Vec<&str> = ver.split('.').collect();
    if parts.len() < 3 { return format!("{}.1", ver); }
    let patch: u32 = parts[2].parse().unwrap_or(0);
    format!("{}.{}.{}", parts[0], parts[1], patch + 1)
}


fn suggest_next_id(devices: &[Device]) -> String {
    let re = regex::Regex::new(r"^([a-zA-Z]+)(\d+)$").unwrap();
    let last = devices.iter().rev().find_map(|d| {
        re.captures(&d.id).map(|c| {
            let prefix = c[1].to_string();
            let num: u32 = c[2].parse().unwrap_or(0);
            let width = c[2].len();
            (prefix, num, width)
        })
    });
    if let Some((prefix, num, width)) = last {
        format!("{}{:0>width$}", prefix, num + 1, width = width)
    } else {
        String::new()
    }
}

fn default_sketch_dir(root: &str, id: &str) -> String {
    let name = format!("ESP32_{}_MQTT_TLS", id.to_uppercase());
    std::path::Path::new(root).join(name).to_string_lossy().to_string()
}

fn resolve_sketch_dir(root: &str, sketch_dir: &str) -> String {
    let p = std::path::Path::new(sketch_dir);
    if p.is_absolute() {
        sketch_dir.to_string()
    } else {
        std::path::Path::new(root).join(sketch_dir).to_string_lossy().to_string()
    }
}

use std::{
    collections::HashMap,
    path::PathBuf,
    sync::{Arc, Mutex, mpsc},
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
    picking_folder: bool,
    first_run: bool,
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
            picking_folder: false,
            first_run: needs_setup,
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
        if self.picking_folder { return; }
        self.picking_folder = true;

        let tx  = self.event_tx.clone();
        let ctx = self.egui_ctx.clone();
        std::thread::spawn(move || {
            let picked = rfd::FileDialog::new()
                .set_title("Select folder")
                .pick_folder();
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

    // ── Event processing ──────────────────────────────────────────────────────

    fn handle_events(&mut self) {
        let events: Vec<AppEvent> = self.event_rx.try_iter().collect();
        for ev in events {
            match ev {
                AppEvent::MqttConnected => {
                    self.mqtt_connected = true;
                }
                AppEvent::MqttDisconnected(e) => {
                    self.mqtt_connected = false;
                    // Reconnect after short delay
                    let cfg = self.config.clone();
                    let arc = self.mqtt_client.clone();
                    let tx  = self.event_tx.clone();
                    let ctx = self.egui_ctx.clone();
                    std::thread::spawn(move || {
                        std::thread::sleep(std::time::Duration::from_secs(5));
                        run_mqtt(cfg, arc, tx, ctx);
                    });
                    let _ = e; // connection error logged silently
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
                            d.log_lines.push(("Uploading to Cloudflare R2…".to_string(), LogLevel::Info));

                            let tx  = self.event_tx.clone();
                            let ctx = self.egui_ctx.clone();
                            let bin = bin_path.unwrap();
                            let did = d.device_id.clone();
                            let ver = d.new_version.clone();
                            let url = self.config.worker_url.clone();
                            let tok = self.config.worker_token.clone();
                            self.rt.spawn(async move {
                                match worker::upload_firmware(bin, did, ver, url, tok).await {
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
                    self.picking_folder = false;
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
                    self.picking_folder = false;
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

    // ── Device grid ───────────────────────────────────────────────────────────

    fn render_devices(&mut self, ui: &mut egui::Ui) {
        if self.fleet.devices.is_empty() {
            ui.add_space(60.0);
            ui.vertical_centered(|ui| {
                ui.colored_label(MUTED, RichText::new("No devices yet").size(16.0));
                ui.add_space(8.0);
                ui.colored_label(MUTED, "Click  + Add Device  to register your first ESP32.");
            });
            return;
        }

        let available_w = ui.available_width();
        let cols = if available_w > 760.0 { 2usize } else { 1 };
        let gap = 12.0;
        let card_w = (available_w - gap * (cols as f32 - 1.0)) / cols as f32;

        let devices: Vec<Device> = self.fleet.devices.clone();
        for chunk in devices.chunks(cols) {
            ui.horizontal(|ui| {
                for (i, device) in chunk.iter().enumerate() {
                    if i > 0 { ui.add_space(gap); }
                    // Force vertical layout inside each card slot so the frame
                    // doesn't inherit the surrounding horizontal context.
                    ui.allocate_ui_with_layout(
                        egui::vec2(card_w, 0.0),
                        egui::Layout::top_down(egui::Align::LEFT),
                        |ui| {
                            self.render_device_card(ui, device);
                        },
                    );
                }
            });
            ui.add_space(10.0);
        }
    }

    fn render_device_card(&mut self, ui: &mut egui::Ui, device: &Device) {
        let raw_status = self.mqtt_status.get(&device.id).cloned().unwrap_or_default();
        let is_online  = raw_status.starts_with("ONLINE");
        let reported   = parse_version_from_status(&raw_status);
        let card_w     = ui.available_width();

        Self::card_frame().show(ui, |ui| {
            // ── Header row: name left, online status right
            ui.horizontal(|ui| {
                let left_w = (card_w - 32.0 - 80.0).max(100.0); // card minus margins minus status
                ui.allocate_ui(egui::vec2(left_w, 0.0), |ui| {
                    ui.label(RichText::new(&device.name).size(15.0).strong().color(TEXT));
                    if !device.company.is_empty() {
                        ui.label(RichText::new(&device.company).size(11.0).color(MUTED));
                    }
                });
                let (dot, col) = if is_online { ("●", SUCCESS) } else { ("○", MUTED) };
                ui.colored_label(col, RichText::new(format!("{} {}", dot, if is_online { "Online" } else { "Offline" })).size(12.0));
            });

            ui.add_space(4.0);
            ui.label(Self::label_mono(format!("solar/{}/…", device.id), MUTED));
            ui.add_space(10.0);
            ui.separator();
            ui.add_space(8.0);

            // ── Version info: flat single line to avoid layout collapse
            let deployed = if device.desired_version.is_empty() {
                "—".to_string()
            } else {
                format!("v{}", device.desired_version)
            };
            ui.horizontal(|ui| {
                ui.colored_label(MUTED, RichText::new("Deployed:").size(11.0));
                ui.label(Self::label_mono(&deployed, TEXT));
                ui.add_space(14.0);
                ui.colored_label(MUTED, RichText::new("Running:").size(11.0));
                if let Some(rv) = &reported {
                    let same = rv == &device.desired_version;
                    let (col, mark) = if same { (SUCCESS, " ✓") } else { (WARNING, " ⚠") };
                    ui.label(Self::label_mono(format!("v{}{}", rv, mark), col));
                } else {
                    ui.label(Self::label_mono("—", MUTED));
                }
            });

            if !device.last_deploy_by.is_empty() {
                ui.add_space(2.0);
                ui.colored_label(MUTED, RichText::new(format!("Last by {}", device.last_deploy_by)).size(11.0));
            }

            ui.add_space(12.0);

            // ── Deploy button
            ui.with_layout(egui::Layout::right_to_left(egui::Align::BOTTOM), |ui| {
                let btn = egui::Button::new(RichText::new("Deploy").color(Color32::WHITE)).fill(PRIMARY);
                if ui.add(btn).clicked() {
                    let dev = device.clone();
                    self.open_deploy(&dev);
                }
            });
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

use std::{
    io::{BufRead, BufReader},
    path::{Path, PathBuf},
    process::{Command, Stdio},
};
use crate::types::{AppEvent, LogLevel};

pub fn read_firmware_version(ino_path: &Path) -> Option<String> {
    let content = std::fs::read_to_string(ino_path).ok()?;
    let re = regex::Regex::new(r#"#define FIRMWARE_VERSION "([0-9.]+)""#).unwrap();
    re.captures(&content)
        .and_then(|c| c.get(1))
        .map(|m| m.as_str().to_string())
}

fn bump_firmware_version(ino_path: &Path, new_version: &str) -> Result<(), String> {
    let content = std::fs::read_to_string(ino_path).map_err(|e| e.to_string())?;
    let re = regex::Regex::new(r#"#define FIRMWARE_VERSION "[0-9.]+""#).unwrap();
    let new_content = re
        .replace(&content, format!(r#"#define FIRMWARE_VERSION "{}""#, new_version))
        .to_string();
    std::fs::write(ino_path, new_content.as_bytes()).map_err(|e| e.to_string())?;
    Ok(())
}

fn send_log(
    tx: &std::sync::mpsc::Sender<AppEvent>,
    ctx: &egui::Context,
    line: impl Into<String>,
    level: LogLevel,
) {
    tx.send(AppEvent::CompileOutput { line: line.into(), level }).ok();
    ctx.request_repaint();
}

fn find_arduino_cli() -> String {
    let candidates = [
        dirs::home_dir()
            .map(|h| h.join(".local/bin/arduino-cli").to_string_lossy().to_string())
            .unwrap_or_default(),
        "/usr/local/bin/arduino-cli".to_string(),
        "/usr/bin/arduino-cli".to_string(),
        "arduino-cli".to_string(), // fallback: rely on PATH
    ];
    for path in &candidates {
        if path.is_empty() { continue; }
        if std::path::Path::new(path).exists() || path == "arduino-cli" {
            return path.clone();
        }
    }
    "arduino-cli".to_string()
}

pub fn compile_sketch(
    sketch_dir: String,
    new_version: String,
    fqbn: String,
    tx: std::sync::mpsc::Sender<AppEvent>,
    ctx: egui::Context,
) {
    // Derive sketch name and .ino path
    let sketch_name = Path::new(&sketch_dir)
        .file_name()
        .map(|n| n.to_string_lossy().to_string())
        .unwrap_or_else(|| "sketch".to_string());

    let ino_path = Path::new(&sketch_dir).join(format!("{}.ino", sketch_name));

    // Validate sketch folder and .ino exist before doing anything
    if !Path::new(&sketch_dir).is_dir() {
        send_log(&tx, &ctx, format!("Error: sketch folder not found: {}", sketch_dir), LogLevel::Error);
        send_log(&tx, &ctx, "→ Set Sketch folder to your Arduino sketch directory, e.g. ~/Arduino/ESP32_VIT010_MQTT_TLS", LogLevel::Info);
        tx.send(AppEvent::CompileDone { success: false, bin_path: None }).ok();
        ctx.request_repaint();
        return;
    }
    if !ino_path.exists() {
        send_log(&tx, &ctx, format!("Error: .ino not found at {}", ino_path.display()), LogLevel::Error);
        send_log(&tx, &ctx, format!("→ The folder name '{}' must match the .ino filename inside it", sketch_name), LogLevel::Info);
        tx.send(AppEvent::CompileDone { success: false, bin_path: None }).ok();
        ctx.request_repaint();
        return;
    }

    // Bump FIRMWARE_VERSION
    send_log(&tx, &ctx, format!("Bumping FIRMWARE_VERSION to {} in {}", new_version, ino_path.display()), LogLevel::Info);
    match bump_firmware_version(&ino_path, &new_version) {
        Ok(()) => send_log(&tx, &ctx, format!("✓ Version set to {}", new_version), LogLevel::Ok),
        Err(e) => {
            send_log(&tx, &ctx, format!("Error updating .ino: {}", e), LogLevel::Error);
            tx.send(AppEvent::CompileDone { success: false, bin_path: None }).ok();
            ctx.request_repaint();
            return;
        }
    }

    // Build output dir
    let ts = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis();
    let tmp = std::env::temp_dir().join(format!("ota-build-{}", ts));
    if let Err(e) = std::fs::create_dir_all(&tmp) {
        send_log(&tx, &ctx, format!("Error creating build dir: {}", e), LogLevel::Error);
        tx.send(AppEvent::CompileDone { success: false, bin_path: None }).ok();
        ctx.request_repaint();
        return;
    }

    let arduino_cli = find_arduino_cli();
    send_log(
        &tx,
        &ctx,
        format!("$ {} compile --fqbn {} --output-dir {} {}", arduino_cli, fqbn, tmp.display(), sketch_dir),
        LogLevel::Info,
    );

    let mut child = match Command::new(&arduino_cli)
        .args([
            "compile",
            "--fqbn",
            &fqbn,
            "--output-dir",
            &tmp.to_string_lossy(),
            &sketch_dir,
        ])
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
    {
        Ok(c) => c,
        Err(e) => {
            send_log(&tx, &ctx, format!("Failed to launch arduino-cli ({}): {}", arduino_cli, e), LogLevel::Error);
            send_log(&tx, &ctx, "Install it: https://arduino.github.io/arduino-cli/latest/installation/", LogLevel::Info);
            tx.send(AppEvent::CompileDone { success: false, bin_path: None }).ok();
            ctx.request_repaint();
            return;
        }
    };

    let stdout = child.stdout.take().unwrap();
    let stderr = child.stderr.take().unwrap();

    let tx1 = tx.clone();
    let ctx1 = ctx.clone();
    let tx2 = tx.clone();
    let ctx2 = ctx.clone();

    let h_out = std::thread::spawn(move || {
        for line in BufReader::new(stdout).lines().flatten() {
            send_log(&tx1, &ctx1, line, LogLevel::Normal);
        }
    });
    let h_err = std::thread::spawn(move || {
        for line in BufReader::new(stderr).lines().flatten() {
            let level = if line.to_lowercase().contains("error:") {
                LogLevel::Error
            } else {
                LogLevel::Normal
            };
            send_log(&tx2, &ctx2, line, level);
        }
    });

    h_out.join().ok();
    h_err.join().ok();

    match child.wait() {
        Ok(status) if status.success() => {
            let bin: PathBuf = tmp.join(format!("{}.ino.bin", sketch_name));
            if bin.exists() {
                send_log(&tx, &ctx, format!("✓ Binary ready: {}", bin.display()), LogLevel::Ok);
                tx.send(AppEvent::CompileDone { success: true, bin_path: Some(bin) }).ok();
            } else {
                send_log(&tx, &ctx, format!("Build succeeded but .bin not found at: {}", bin.display()), LogLevel::Error);
                tx.send(AppEvent::CompileDone { success: false, bin_path: None }).ok();
            }
        }
        Ok(_) => {
            send_log(&tx, &ctx, "Compilation failed — see errors above".to_string(), LogLevel::Error);
            tx.send(AppEvent::CompileDone { success: false, bin_path: None }).ok();
        }
        Err(e) => {
            send_log(&tx, &ctx, format!("Process error: {}", e), LogLevel::Error);
            tx.send(AppEvent::CompileDone { success: false, bin_path: None }).ok();
        }
    }

    ctx.request_repaint();
}

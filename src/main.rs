#![cfg_attr(not(debug_assertions), windows_subsystem = "windows")]

mod app;
mod compile;
mod config;
mod mqtt;
mod types;
mod worker;

use std::sync::Arc;

fn main() -> eframe::Result<()> {
    let rt = Arc::new(tokio::runtime::Runtime::new().expect("tokio runtime"));

    let options = eframe::NativeOptions {
        viewport: egui::ViewportBuilder::default()
            .with_title("OTA Flasher")
            .with_inner_size([1080.0, 680.0])
            .with_min_inner_size([800.0, 560.0]),
        ..Default::default()
    };

    eframe::run_native(
        "OTA Flasher",
        options,
        Box::new(move |cc| Ok(Box::new(app::App::new(cc, rt.clone())))),
    )
}

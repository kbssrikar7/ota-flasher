use rumqttc::{Client, ConnAck, Event, MqttOptions, Packet, QoS, TlsConfiguration, Transport};
use std::sync::{Arc, Mutex};
use crate::types::{AppConfig, AppEvent};

pub fn run_mqtt(
    config: AppConfig,
    client_arc: Arc<Mutex<Option<Client>>>,
    tx: std::sync::mpsc::Sender<AppEvent>,
    ctx: egui::Context,
) {
    let ts = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis();

    let mut opts = MqttOptions::new(
        format!("ota-flasher-{}", ts),
        config.mqtt_host.clone(),
        config.mqtt_port,
    );
    opts.set_credentials(config.mqtt_user.clone(), config.mqtt_pass.clone());
    opts.set_transport(Transport::Tls(TlsConfiguration::default()));
    opts.set_keep_alive(std::time::Duration::from_secs(30));

    let (client, mut connection) = Client::new(opts, 20);
    *client_arc.lock().unwrap() = Some(client.clone());

    if let Err(_) = client.subscribe("solar/+/status", QoS::AtLeastOnce) {
        tx.send(AppEvent::MqttDisconnected).ok();
        *client_arc.lock().unwrap() = None;
        ctx.request_repaint();
        return;
    }

    for notification in connection.iter() {
        match notification {
            Ok(Event::Incoming(Packet::ConnAck(ConnAck { code, .. }))) => {
                if code == rumqttc::ConnectReturnCode::Success {
                    tx.send(AppEvent::MqttConnected).ok();
                    ctx.request_repaint();
                } else {
                    tx.send(AppEvent::MqttDisconnected).ok();
                    *client_arc.lock().unwrap() = None;
                    ctx.request_repaint();
                    break;
                }
            }
            Ok(Event::Incoming(Packet::Publish(p))) => {
                let payload = String::from_utf8_lossy(&p.payload).to_string();
                let parts: Vec<&str> = p.topic.split('/').collect();
                if parts.len() >= 3 {
                    tx.send(AppEvent::MqttStatus {
                        device_id: parts[1].to_string(),
                        status: payload,
                    })
                    .ok();
                    ctx.request_repaint();
                }
            }
            Err(_) => {
                tx.send(AppEvent::MqttDisconnected).ok();
                *client_arc.lock().unwrap() = None;
                ctx.request_repaint();
                break;
            }
            _ => {}
        }
    }
}

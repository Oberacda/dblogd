extern crate mosquitto_client as mosq;

use std::sync::mpsc::Sender;
use crate::record::TemperatureRecord;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct MqttParams
{
    /// The ip address the socket should listen on.
    pub address: String,
    /// The port the socket should listen on.
    pub port: u32,

    pub env_topic: String,

    pub qos: u32,
}

pub fn thread_mqtt(tx: Sender<TemperatureRecord>, thread_finish: Arc<AtomicBool>, params: MqttParams)
{
    let mqtt_client = mosq::Mosquitto::new("dblogd");

    match mqtt_client.connect(params.address.as_ref(), params.port) {
        Ok(_) => {
            log::info!(target: "dblogd::mqtt", "Connected to mqtt client!");
        },
        Err(err) => {
            log::error!(target: "dblogd::mqtt", "Unable to connect: \'{}\'", err);
            thread_finish.store(true, Ordering::SeqCst);
            return;
        }
    }

    let env_packages = match mqtt_client.subscribe(params.env_topic.as_ref(), params.qos)  {
        Ok(res) => res,
        Err(err) => {
            log::error!(target: "dblogd::mqtt", "Unable to subscribe: \'{}\'", err);
            match mqtt_client.disconnect() {
                Ok(_) => {
                    log::warn!(target: "dblogd::mqtt", "Disconnected mqtt client!");
                }
                Err(err) => {
                    log::error!(target: "dblogd::mqtt", "Unable to disconnect: \'{}\'", err);
                    thread_finish.store(true, Ordering::SeqCst);
                }
            };
            thread_finish.store(true, Ordering::SeqCst);
            return;
        }
    };

    let mut mqtt_client_callbacks = mqtt_client.callbacks(());

    mqtt_client_callbacks.on_message(|_,msg| {
        if ! msg.retained() { // not interested in any retained messages!
            if env_packages.matches(&msg) {
                let recv_string = match std::str::from_utf8(msg.payload()) {
                    Ok(string) => String::from(string),
                    Err(err) => {
                        log::warn!(target: "dblogd::mqtt", "Socket received non UTF-8 data: \'{}\'", err);
                        return;
                    }
                };

                let recv_data_str_trimmed = recv_string.trim_end();

                let json_buf_record = match serde_json::from_str::<TemperatureRecord>(recv_data_str_trimmed) {
                    Ok(result) => result,
                    Err(err) => {
                        log::error!(target: "dblogd::mqtt", "Recieved data cannot be deserialized via JSON: \'{}\'", err);
                        return;
                    }
                };
                match tx.send(json_buf_record) {
                    Ok(_) => log::debug!(target: "dblogd::mqtt", "Send message to database thread!"),
                    Err(err) => {
                        log::error!(target: "dblogd::mqtt", "Could not send message to database thread: \'{}\'", err);
                    }
                };
            }
        }
    });

    let timeout: i32 = 100;
    while !thread_finish.load(Ordering::SeqCst) {
        match mqtt_client.do_loop(timeout) {
            Ok(_) => {
                log::debug!(target: "dblogd::mqtt", "Running mqtt loop!")
            },
            Err(err) => {
                log::error!(target: "dblogd::mqtt", "Unable to run mqtt loop: \'{}\'", err);
                match mqtt_client.disconnect() {
                    Ok(_) => {
                        log::warn!(target: "dblogd::mqtt", "Disconnected mqtt client!");
                    }
                    Err(err) => {
                        log::error!(target: "dblogd::mqtt", "Unable to disconnect: \'{}\'", err);
                        thread_finish.store(true, Ordering::SeqCst);
                    }
                };
                thread_finish.store(true, Ordering::SeqCst);
                return;
            }
        };
    }
}
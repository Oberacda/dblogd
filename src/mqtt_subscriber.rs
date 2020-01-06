extern crate mosquitto_client as mosq;

use std::sync::mpsc::Sender;
use crate::record::TemperatureRecord;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use serde::{Deserialize, Serialize};

use std::time;

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct MqttParams
{
    /// The ip address the socket should listen on.
    pub address: String,
    /// The port the socket should listen on.
    pub port: u32,
}

pub fn thread_mqtt(tx: Sender<TemperatureRecord>, thread_finish: Arc<AtomicBool>, params: MqttParams)
{
    let mqtt_client = mosq::Mosquitto::new("raspberry3");

    match mqtt_client.connect(params.address.as_ref(), params.port) {
        Ok(_) => {},
        Err(err) => {
            log::error!(target: "dblogd::mqtt", "Unable to connect: \'{}\'", err);
            thread_finish.store(true, Ordering::SeqCst);
            return;
        }
    }

    let temperature_packages = match mqtt_client.subscribe("david/env/temperature", 1)  {
        Ok(res) => res,
        Err(err) => {
            log::error!(target: "dblogd::mqtt", "Unable to subscribe: \'{}\'", err);
            match mqtt_client.disconnect() {
                Ok(_) => {}
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
            if temperature_packages.matches(&msg) {
                println!("temperature {:?}",msg);
            }
        }
    });

    let timeout = time::Duration::from_millis(100);
    while !thread_finish.load(Ordering::SeqCst) {
        mqtt_client.do_loop(timeout.as_millis() as i32);
    }
}
extern crate mosquitto_client as mosq;

use std::sync::mpsc::Sender;
use crate::record::EnvironmentalRecord;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use serde::{Deserialize, Serialize};
use std::time::SystemTime;

#[derive(Serialize, Deserialize, Debug, Clone)]
/// Parameters for the mqtt connection.
pub struct MqttParams
{
    /// The ip address the socket should listen on.
    pub address: String,
    /// The port the socket should listen on.
    pub port: u32,
    /// Enable tls encryption.
    pub tls_enable: bool,
    /// The path to the CA certificate for TLS encryption.
    pub ca_path: Option<String>,
    /// The path to the certificate to use for TLS encryption.
    pub cert_path: Option<String>,
    /// The path to the key to use for TLS encryption.
    pub key_path: Option<String>,
    /// The password for the ssl private key.å
    pub key_pass: Option<String>,
    /// Topic to subscribe to fr environmental data.
    pub env_topic: String,
    /// The QoS to use for the subscription.
    pub qos: u32,
}

pub fn thread_mqtt(tx: Sender<EnvironmentalRecord>, thread_finish: Arc<AtomicBool>, params: MqttParams)
{
    let current_unix_timestamp = match SystemTime::now().duration_since(SystemTime::UNIX_EPOCH) {
        Ok(n) => n.as_secs(),
        Err(_) => {
            log::error!(target: "dblogd::mqtt", "Invalid system time. Its before the UNIX_EPOCH");
            thread_finish.store(true, Ordering::SeqCst);
            return;
        }
    };

    let mqtt_client = mosq::Mosquitto::new(format!("dblogd-{}", current_unix_timestamp).as_ref());
    mqtt_client.threaded();
    if params.tls_enable {
        let ca_path = match params.ca_path {
            Some(ca_path) => ca_path,
            None => {
                log::error!(target: "dblogd::mqtt", "TLS enabled but no CA file specified!");
                thread_finish.store(true, Ordering::SeqCst);
                return;
            }
        };
        let cert_path = match params.cert_path {
            Some(cert_path) => cert_path,
            None => {
                log::error!(target: "dblogd::mqtt", "TLS enabled but no Certificate file specified!");
                thread_finish.store(true, Ordering::SeqCst);
                return;
            }
        };

        let key_path = match params.key_path {
            Some(key_path) => key_path,
            None => {
                log::error!(target: "dblogd::mqtt", "TLS enabled but no private key file specified!");
                thread_finish.store(true, Ordering::SeqCst);
                return;
            }
        };

        let key_pass = match params.key_pass {
            Some(key_pass) => key_pass,
            None => {
                log::error!(target: "dblogd::mqtt", "TLS enabled but no private key password specified!");
                thread_finish.store(true, Ordering::SeqCst);
                return;
            }
        };

        match mqtt_client.tls_set(ca_path.as_str(), cert_path.as_str(), key_path.as_str(),Option::Some(key_pass.as_str())) {
            Ok(_) => {
                log::debug!(target: "dblogd::mqtt", "Set tls parameters for connection!");
            },
            Err(err) => {
                log::error!(target: "dblogd::mqtt", "Could not set tls parameters for connection: \'{}\'", err);
                thread_finish.store(true, Ordering::SeqCst);
                return;
            }
        };
    }

    let mqtt_connected = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let mqtt_connected_callback = Arc::clone(&mqtt_connected);

    let mut mqtt_client_callbacks = mqtt_client.callbacks(());

    mqtt_client_callbacks.on_connect(  | _, code| {
        match code {
            0 => {
                log::info!(target: "dblogd::mqtt", "Connected to mqtt client!");
                mqtt_connected_callback.store(true, Ordering::SeqCst);
            }
            _ => {
                log::error!(target: "dblogd::mqtt", "Can not connected to mqtt client!");
                thread_finish.store(true, Ordering::SeqCst);
                return;
            }
        }
    });

    match mqtt_client.connect(params.address.as_ref(), params.port) {
        Ok(_) => {
            log::info!(target: "dblogd::mqtt", "Started connecting to mqtt client!");
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
            thread_finish.store(true, Ordering::SeqCst);
            return;
        }
    };


    while !mqtt_connected.load(Ordering::SeqCst) {
        if  thread_finish.load(Ordering::SeqCst) {
            log::error!(target: "dblogd::mqtt", "Exiting mqtt loop");
            return;
        }
        match mqtt_client.do_loop(100) {
            Ok(_) => {
                log::trace!(target: "dblogd::mqtt", "Running mqtt loop!")
            },
            Err(err) => {
                log::error!(target: "dblogd::mqtt", "Unable to run mqtt loop: \'{}\'", err);
            }
        };
    }

    mqtt_client_callbacks.on_message( move |_,msg| {
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

                let json_buf_record = match serde_json::from_str::<EnvironmentalRecord>(recv_data_str_trimmed) {
                    Ok(result) => result,
                    Err(err) => {
                        log::error!(target: "dblogd::mqtt", "Received data cannot be deserialized via JSON: \'{}\'", err);
                        return;
                    }
                };
                match tx.send(json_buf_record) {
                    Ok(_) => log::trace!(target: "dblogd::mqtt", "Send message to database thread!"),
                    Err(err) => {
                        log::error!(target: "dblogd::mqtt", "Could not send message to database thread: \'{}\'", err);
                    }
                };
            } else {
                log::warn!(target: "dblogd::mqtt", "Received invalid package on the channel!")
            }
        }
    });

    while !thread_finish.load(Ordering::SeqCst) {
        match mqtt_client.do_loop(100) {
            Ok(_) => {
                log::trace!(target: "dblogd::mqtt", "Running mqtt loop!")
            },
            Err(err) => {
                log::trace!(target: "dblogd::mqtt", "Unable to run mqtt loop: \'{}\'", err);
            }
        };
    }
}
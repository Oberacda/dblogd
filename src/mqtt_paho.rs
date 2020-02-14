extern crate paho_mqtt as mqtt;

use std::sync::mpsc::Sender;
use crate::record::EnvironmentalRecord;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use serde::{Deserialize, Serialize};
use std::time::SystemTime;
use std::time;

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
    /// Optional TLS parameters for the mqtt connection.
    pub tls_params: Option<MqttTlsParams>,
    /// Topic to subscribe to fr environmental data.
    pub env_topic: String,
    /// The QoS to use for the subscription.
    pub qos: i32,
}
#[derive(Serialize, Deserialize, Debug, Clone)]
/// TLS parametes required for MQTT with TLS.
pub struct MqttTlsParams {
    /// The path to the CA certificate for TLS encryption.
    pub ca_path: String,
    /// The path to the certificate to use for TLS encryption.
    pub cert_path: String,
    /// The path to the key to use for TLS encryption.
    pub key_path: String,
    /// The password for the ssl private key.
    pub key_pass: Option<String>,
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

    let connection_string= match params.tls_enable {
        true => {
            format!("ssl://{}:{}", params.address, params.port)
        }
        false => {
            format!("tcp://{}:{}", params.address, params.port)
        }
    };

    let create_opts = mqtt::CreateOptionsBuilder::new()
        .server_uri(connection_string)
        .client_id(format!("dblogd-{}", current_unix_timestamp))
        .finalize();

    let mut mqtt_client = match mqtt::Client::new(create_opts) {
        Ok(client) => client,
        Err(err) => {
            log::error!(target: "dblogd::mqtt", "Could not create mqtt client: \'{}\'!", err);
            thread_finish.store(true, Ordering::SeqCst);
            return;
        }
    };
    mqtt_client.set_timeout(std::time::Duration::from_millis(4000));

    let connection_opts = match params.tls_enable {
        true => {
            let tls_params = match params.tls_params {
                Some(tls_params) => tls_params,
                None => {
                    log::error!(target: "dblogd::mqtt", "TLS enabled but no TLS parameters specified!");
                    thread_finish.store(true, Ordering::SeqCst);
                    return;
                }
            };

            let ssl_options = match tls_params.key_pass {
                Some(key_pass) => {
                    mqtt::SslOptionsBuilder::new()
                        .trust_store(tls_params.ca_path.as_ref())
                        .key_store(tls_params.cert_path.as_ref())
                        .private_key(tls_params.key_path.as_ref())
                        .private_key_password(key_pass.as_ref())
                        .finalize()
                },
                None => {
                    mqtt::SslOptionsBuilder::new()
                        .trust_store(tls_params.ca_path.as_ref())
                        .key_store(tls_params.cert_path.as_ref())
                        .private_key(tls_params.key_path.as_ref())
                        .finalize()
                }
            };
            mqtt::ConnectOptionsBuilder::new()
                .connect_timeout(time::Duration::from_millis(4000))
                .ssl_options(ssl_options)
                .finalize()
        },
        false => {
            mqtt::ConnectOptionsBuilder::new()
                .connect_timeout(time::Duration::from_millis(4000))
                .finalize()
        }
    };

    match mqtt_client.connect(connection_opts) {
        Ok((str, status, conn)) => {
            log::info!(target: "dblogd::mqtt", "Mqtt client connected: \'{}\', \'{}\', \'{}\'", str, status, conn);
        },
        Err(err) => {
            log::error!(target: "dblogd::mqtt", "Unable to connect: \'{}\'", err);
            thread_finish.store(true, Ordering::SeqCst);
            return;
        }
    };

    let reciever_queue = mqtt_client.start_consuming();

    match mqtt_client.subscribe(params.env_topic.as_ref(), params.qos) {
        Ok(res) => {
            log::debug!(target: "dblogd::mqtt", "subscribed to topic {} with qos {}: \'{}\'", params.env_topic, params.qos, res);
        }
        Err(err) => {
            log::error!(target: "dblogd::mqtt", "Unable to subscribe: \'{}\'", err);
            match mqtt_client.disconnect(Option::None) {
                Ok(_) => log::info!(target: "dblogd::mqtt", "Disconnected from mqtt client!"),
                Err(err) =>  log::error!(target: "dblogd::mqtt", "Could not disconnected from mqtt client: {}", err)
            }
            thread_finish.store(true, Ordering::SeqCst);
            return;
        }
    };

    let timeout = time::Duration::from_millis(100);

    while !thread_finish.load(Ordering::SeqCst) {
        let envirnonement_record_opt = match reciever_queue.recv_timeout(timeout) {
            Ok(record) => {
                record
            }
            Err(_) => {
                continue;
            }
        };

        match envirnonement_record_opt {
            Some(message) => {
                let recv_string = match std::str::from_utf8(message.payload()) {
                    Ok(string) => String::from(string),
                    Err(err) => {
                        log::warn!(target: "dblogd::mqtt", "Socket received non UTF-8 data: \'{}\'", err);
                        continue;
                    }
                };

                let recv_data_str_trimmed = recv_string.trim_end();

                let json_buf_record = match serde_json::from_str::<EnvironmentalRecord>(recv_data_str_trimmed) {
                    Ok(result) => result,
                    Err(err) => {
                        log::warn!(target: "dblogd::mqtt", "Received data cannot be deserialized via JSON: \'{}\'", err);
                        continue;
                    }
                };
                match tx.send(json_buf_record) {
                    Ok(_) => log::trace!(target: "dblogd::mqtt", "Send message to database thread!"),
                    Err(err) => {
                        log::error!(target: "dblogd::mqtt", "Could not send message to database thread: \'{}\'", err);
                        thread_finish.store(true, Ordering::SeqCst);
                        return;
                    }
                };
            },
            None => {
                match mqtt_client.reconnect() {
                    Ok((str, status, conn)) => {
                        log::info!(target: "dblogd::mqtt", "Mqtt client reconnected: \'{}\', \'{}\', \'{}\'", str, status, conn);
                    },
                    Err(err) => {
                        log::error!(target: "dblogd::mqtt", "Unable to reconnect: \'{}\'", err);
                        thread_finish.store(true, Ordering::SeqCst);
                        return;
                    }
                }
            }
        }
    }

    match mqtt_client.disconnect(Option::None) {
        Ok(_) => log::info!(target: "dblogd::mqtt", "Disconnected from mqtt client!"),
        Err(err) =>  log::error!(target: "dblogd::mqtt", "Could not disconnected from mqtt client: {}", err)
    };
    return;
}
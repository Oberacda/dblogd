use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::mpsc::Receiver;
use std::time;

use postgres::{Connection, TlsMode};
use postgres_openssl::OpenSsl;
use postgres_openssl::openssl::ssl::{SslConnector, SslFiletype, SslMethod};
use serde::{Deserialize, Serialize};

use crate::record::TemperatureRecord;

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct DatabaseParameters {
    pub hostname: String,
    pub port: u32,
    pub username: String,
    pub password: String,
    pub database: String,
    pub server_ca_path: String,
    pub client_cert_path: String,
    pub client_key_path: String,
}

pub fn database_thread(rx: Receiver<TemperatureRecord>, thread_finished: Arc<AtomicBool>, connection_parameters: DatabaseParameters) {
    let mut ssl_connection_builder = match SslConnector::builder(SslMethod::tls()) {
        Ok(builder) => builder,
        Err(err) => {
            log::error!(target: "dblogd::db", "Could not create ssl connection builder: \'{}\'", err);
            thread_finished.store(true, Ordering::SeqCst);
            return;
        }
    };

    match ssl_connection_builder.set_ca_file(connection_parameters.server_ca_path) {
        Ok(_) => {}
        Err(err) => {
            log::error!(target: "dblogd::db", "Could not set ssl ca file: \'{}\'", err);
            thread_finished.store(true, Ordering::SeqCst);
            return;
        }
    };

    match ssl_connection_builder.set_certificate_file(connection_parameters.client_cert_path, SslFiletype::PEM) {
        Ok(_) => {}
        Err(err) => {
            log::error!(target: "dblogd::db", "Could not set ssl client cert file: \'{}\'", err);
            thread_finished.store(true, Ordering::SeqCst);
            return;
        }
    };

    match ssl_connection_builder.set_private_key_file(connection_parameters.client_key_path, SslFiletype::PEM) {
        Ok(_) => {}
        Err(err) => {
            log::error!(target: "dblogd::db", "Could not set ssl client key file: \'{}\'", err);
            thread_finished.store(true, Ordering::SeqCst);
            return;
        }
    };

    let mut negotiator: OpenSsl = OpenSsl::from(ssl_connection_builder.build());

    //TODO: Remove once the hostname verification can be ensured!
    negotiator.danger_disable_hostname_verification(true);

    let postgres_connection_string = format!("postgresql://{}:{}@{}:{}/{}?application_name=dblogd",
                                             connection_parameters.username,
                                             connection_parameters.password,
                                             connection_parameters.hostname,
                                             connection_parameters.port,
                                             connection_parameters.database);


    let database_connection: Connection = match Connection::connect(postgres_connection_string, TlsMode::Require(&negotiator))
        {
            Ok(conn) => conn,
            Err(err) => {
                log::error!(target: "dblogd::db", "Could not establish database connection: \'{}\'", err);
                thread_finished.store(true, Ordering::SeqCst);
                return;
            }
        };
    log::info!(target: "dblogd::db", "Database connection established!");
    let timeout = time::Duration::from_millis(100);

    while !thread_finished.load(Ordering::SeqCst) {
        let mut temperature_record = match rx.recv_timeout(timeout) {
            Ok(record) => record,
            Err(_) => {
                continue;
            }
        };
        let probe_rows = match database_connection.query("INSERT INTO sensors.records (timestamp, sensor_name) VALUES ($1, $2) RETURNING id",
                                                         &[&temperature_record.timestamp, &temperature_record.sensor_name]) {
            Ok(rows) => rows,
            Err(err) => {
                log::warn!(target: "dblog::db", "Could not insert probe into database: \'{}\'", err);
                continue;
            }
        };

        let new_id: i64 = probe_rows.get(0).get("id");
        temperature_record.id = new_id;

        match database_connection.execute("INSERT INTO sensors.temperature (record_id, celsius) VALUES ($1, $2)",
                                          &[&temperature_record.id, &temperature_record.celsius]) {
            Ok(_) => {}
            Err(err) => {
                log::warn!(target: "dblog::db", "Could not insert celsius value into database: \'{}\'", err);
                continue;
            }
        }
        match database_connection.execute("INSERT INTO sensors.humidity (record_id, humidity) VALUES ($1, $2)",
                                          &[&temperature_record.id, &temperature_record.humidity]) {
            Ok(_) => {}
            Err(err) => {
                log::warn!(target: "dblog::db", "Could not insert celsius value into database: \'{}\'", err);
                continue;
            }
        }
    }
}
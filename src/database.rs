//! Module for connecting to a postgres database and storing the records received from a socket in
//! the database.
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::mpsc::Receiver;
use std::time;

use openssl::ssl::{SslConnector, SslFiletype, SslMethod, SslVerifyMode};
use postgres::Client;
use postgres_openssl::MakeTlsConnector;
use serde::{Deserialize, Serialize};

use crate::record::EnvironmentalRecord;
use chrono::{Utc, Local, TimeZone, DateTime};

static SQL_CREATE_DATABASE: &'static str = include_str!("sql/create_database.sql");

static SQL_SELECT_SENSOR_ID: &'static str = include_str!("sql/select_sensor_id.sql");

static SQL_INSERT_RECORD: &'static str = include_str!("sql/insert_record.sql");
static SQL_INSERT_HUMIDITY: &'static str = include_str!("sql/insert_humidity.sql");
static SQL_INSERT_ILLUMINANCE: &'static str = include_str!("sql/insert_illuminance.sql");
static SQL_INSERT_PRESSURE: &'static str = include_str!("sql/insert_pressure.sql");
static SQL_INSERT_TEMPERATURE: &'static str = include_str!("sql/insert_temperature.sql");
static SQL_INSERT_UV_INDEX: &'static str = include_str!("sql/insert_uv_index.sql");
static SQL_INSERT_UVA: &'static str = include_str!("sql/insert_uva.sql");
static SQL_INSERT_UVB: &'static str = include_str!("sql/insert_uvb.sql");

#[derive(Serialize, Deserialize, Debug, Clone)]
/// Struct modeling the parameters required for a database connection.
///
/// This includes SSL/TLS encryption.
pub struct DatabaseParameters
{
    /// The hostname of the database server.
    pub hostname: String,
    /// The port for the database server.
    pub port: u32,
    /// The username to connect as.
    pub username: String,
    /// The password to connect with.
    pub password: String,
    /// The database to open on the server.
    pub database: String,
    /// Flag to enable tls for the database server connection.
    pub tls_enable: bool,
    /// Parameters for the tls connection to the database server.
    pub tls_params: Option<DatabaseTlsParameters>,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
/// Struct for the parameters required for a tls connection to the database.
pub struct DatabaseTlsParameters {
    /// The path to the server certificate for TLS encryption.
    pub server_ca_path: String,
    /// The path to the client certificate for TLS encryption.
    pub client_cert_path: String,
    /// The path to the client key for TLS encryption.
    pub client_key_path: String,
}

/// Function to insert a temperature record into the database.
///
/// # Arguments
///
/// * `database_client` - Database connection to execute the queries on.
///
/// * `temperature_record` - The record to add to the database.
///
/// # Returns
///
/// * `Ok(())` - On success.
///
/// * `Err(...)` - If a single operation fails.
///     Failing operations can be if a record cannot be inserted into the database.
///     The sensor with this name does not exist.
///
fn insert_temperature_record(database_client: &mut Client, env_record: EnvironmentalRecord) -> Result<(), String>
{
    let sensor_name_query_results = match database_client.query(SQL_SELECT_SENSOR_ID, &[&env_record.sensor_name]) {
        Ok(rows) => rows,
        Err(err) => {
            log::warn!(target: "dblogd::db", "Could not find sensor name in known sensors: \'{}\'", err);
            return Err(String::from("Could not find sensor nama in known sensors!"));
        }
    };

    if sensor_name_query_results.len() != 1 {
        log::warn!(target: "dblogd::db", "Found non unique sensor name, please ensure database consistency!");
        return Err(String::from("Found non unique sensor name, please ensure database consistency!"));
    };


    let timestamp_datetime_local: DateTime<Local> = Local.timestamp(env_record.timestamp, 0);
    let timestamp_datetime: DateTime<Utc> = DateTime::<Utc>::from(timestamp_datetime_local);

    let sensor_name_id: i64 = sensor_name_query_results.get(0).unwrap().get("id");

    let new_records_result = match database_client.query(SQL_INSERT_RECORD,
                                                         &[&timestamp_datetime, &sensor_name_id]) {
        Ok(rows) => rows,
        Err(err) => {
            log::warn!(target: "dblog::db", "Could not insert record into database: \'{}\'", err);
            return Err(String::from("Could not insert record into database"));
        }
    };

    if new_records_result.len() != 1 {
        log::warn!(target: "dblogd::db", "Found non unique record id result, please ensure database consistency!");
        return Err(String::from("Found non unique record id result, please ensure database consistency!"));
    };

    let new_record_id: i64 = new_records_result.get(0).unwrap().get("id");

    match database_client.execute(SQL_INSERT_TEMPERATURE,
                                  &[&new_record_id, &env_record.temperature]) {
        Ok(_) => {}
        Err(err) => {
            log::warn!(target: "dblog::db", "Could not insert celsius value into database: \'{}\'", err);
            return Err(String::from("Could not insert celsius value into database"));
        }
    };

    match database_client.execute(SQL_INSERT_HUMIDITY,
                                  &[&new_record_id, &env_record.humidity]) {
        Ok(_) => {}
        Err(err) => {
            log::warn!(target: "dblog::db", "Could not insert humidity value into database: \'{}\'", err);
            return Err(String::from("Could not insert humidity value into database"));
        }
    };

    match database_client.execute(SQL_INSERT_PRESSURE,
                                  &[&new_record_id, &env_record.pressure]) {
        Ok(_) => {}
        Err(err) => {
            log::warn!(target: "dblog::db", "Could not insert pressure value into database: \'{}\'", err);
            return Err(String::from("Could not insert pressure value into database"));
        }
    };

    match database_client.execute(SQL_INSERT_ILLUMINANCE,
                                  &[&new_record_id, &env_record.illuminance]) {
        Ok(_) => {}
        Err(err) => {
            log::warn!(target: "dblog::db", "Could not insert illuminance value into database: \'{}\'", err);
            return Err(String::from("Could not insert illuminance value into database"));
        }
    };

    match database_client.execute(SQL_INSERT_UVA,
                                  &[&new_record_id, &env_record.uva]) {
        Ok(_) => {}
        Err(err) => {
            log::warn!(target: "dblog::db", "Could not insert uva value into database: \'{}\'", err);
            return Err(String::from("Could not insert uva value into database"));
        }
    };

    match database_client.execute(SQL_INSERT_UVB,
                                  &[&new_record_id, &env_record.uvb]) {
        Ok(_) => {}
        Err(err) => {
            log::warn!(target: "dblog::db", "Could not insert uvb value into database: \'{}\'", err);
            return Err(String::from("Could not insert uvb value into database"));
        }
    };

    match database_client.execute(SQL_INSERT_UV_INDEX,
                                  &[&new_record_id, &env_record.uv_index]) {
        Ok(_) => {}
        Err(err) => {
            log::warn!(target: "dblog::db", "Could not insert uv_index value into database: \'{}\'", err);
            return Err(String::from("Could not insert uv_index value into database"));
        }
    };

    Ok(())
}

/// Thread function for the database connection.
///
/// This thread establishes a database connection and moves all data in the receive channel to the database.
///
/// This function will run until the `thread_finish` parameter was set or the socket is closed by a error.
///
/// # Arguments
///
/// * `rx` - The channel to receive the elements to insert from.
///
/// * `thread_finish` - Indicates that the thread should finish operation and should return.
///
/// * `connection_parameters` - Parameters for the database connection.
///
/// # Errors
///
/// Errors occur when one of the following conditions is met:
///
/// * The files for the TLS connection cannot be found.
///
/// * The connection cannot be established.
///
/// * The the user is not authorized for the database.
///
/// These errors will result in the method immediately exiting without raising a exception.
///
pub fn database_thread(rx: Receiver<EnvironmentalRecord>, thread_finish: Arc<AtomicBool>, connection_parameters: DatabaseParameters)
{
    let postgres_connection_string = format!("user={} password={} host={} port={} dbname={} application_name=dblogd",
                                             connection_parameters.username,
                                             connection_parameters.password,
                                             connection_parameters.hostname,
                                             connection_parameters.port,
                                             connection_parameters.database);

    let mut database_connection: Client = match connection_parameters.tls_enable {
        true => {
            let tls_params = match connection_parameters.tls_params {
                Some(tls_params) => tls_params,
                None => {
                    log::error!(target: "dblogd::db", "TLS enabled but no TLS parameters specified!");
                    thread_finish.store(true, Ordering::SeqCst);
                    return;
                }
            };

            let mut ssl_connection_builder: openssl::ssl::SslConnectorBuilder = match SslConnector::builder(SslMethod::tls()) {
                Ok(builder) => builder,
                Err(err) => {
                    log::error!(target: "dblogd::db", "Could not create ssl connection builder: \'{}\'", err);
                    thread_finish.store(true, Ordering::SeqCst);
                    return;
                }
            };
        
            ssl_connection_builder.set_verify(SslVerifyMode::NONE);
        
            match ssl_connection_builder.set_ca_file(tls_params.server_ca_path) {
                Ok(_) => {}
                Err(err) => {
                    log::error!(target: "dblogd::db", "Could not set ssl ca file: \'{}\'", err);
                    thread_finish.store(true, Ordering::SeqCst);
                    return;
                }
            };
        
            match ssl_connection_builder.set_certificate_file(tls_params.client_cert_path, SslFiletype::PEM) {
                Ok(_) => {}
                Err(err) => {
                    log::error!(target: "dblogd::db", "Could not set ssl client cert file: \'{}\'", err);
                    thread_finish.store(true, Ordering::SeqCst);
                    return;
                }
            };
        
            match ssl_connection_builder.set_private_key_file(tls_params.client_key_path, SslFiletype::PEM) {
                Ok(_) => {}
                Err(err) => {
                    log::error!(target: "dblogd::db", "Could not set ssl client key file: \'{}\'", err);
                    thread_finish.store(true, Ordering::SeqCst);
                    return;
                }
            };
            let tls_connector = MakeTlsConnector::new(ssl_connection_builder.build());
            match Client::connect(postgres_connection_string.as_str(), tls_connector)
            {
                Ok(conn) => conn,
                Err(err) => {
                    log::error!(target: "dblogd::db", "Could not establish database connection: \'{}\'", err);
                    thread_finish.store(true, Ordering::SeqCst);
                    return;
                }
            }
        },
        false => {
            match Client::connect(postgres_connection_string.as_str(), postgres::NoTls)
            {
                Ok(conn) => conn,
                Err(err) => {
                    log::error!(target: "dblogd::db", "Could not establish database connection: \'{}\'", err);
                    thread_finish.store(true, Ordering::SeqCst);
                    return;
                }
            }
        }
    };
     
    log::info!(target: "dblogd::db", "Database connection established!");
    let timeout = time::Duration::from_millis(100);

    while !thread_finish.load(Ordering::SeqCst) {
        let temperature_record = match rx.recv_timeout(timeout) {
            Ok(record) => {
                record
            }
            Err(_) => {
                continue;
            }
        };

        match insert_temperature_record(&mut database_connection, temperature_record) {
            Ok(_) => {}
            Err(err) => {
                log::error!(target: "dblogd::db", "Database insert failed: \'{}\'", err);
                continue;
            }
        }
    }
}
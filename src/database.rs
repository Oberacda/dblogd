//! Module for connecting to a postgres database and storing the records received from a socket in
//! the database.
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::mpsc::Receiver;
use std::time;

use bincode;

use byteorder::ByteOrder;
use byteorder::BigEndian;
use leveldb::database::Database;
use leveldb::kv::KV;
use leveldb::options::{Options,WriteOptions};
use db_key;
use serde::{Deserialize, Serialize};
use chrono::Utc;

use crate::record::EnvironmentalRecord;

#[derive(Serialize, Deserialize, Debug, Clone)]
/// Struct modeling the parameters required for a database connection.
///
/// This includes SSL/TLS encryption.
pub struct DatabaseParameters
{
    /// The filename for the database file.
    pub filename: String,
    /// Should the database be created if it does not exitst already.
    pub create_if_nonexistent: bool
}

struct TimestampKey {
    milliseconds: [u8; 8]
}

impl<'a> std::convert::From<&'a chrono::DateTime<Utc>> for TimestampKey {
    fn from(key: &'a chrono::DateTime<Utc>) -> Self {
        let mut buf: [u8; 8] = [0; 8];
        BigEndian::write_i64(&mut buf, key.timestamp_millis());
        return TimestampKey {
            milliseconds: buf
        }
    }
}

impl std::convert::From<i64> for TimestampKey {
    fn from(value: i64) -> Self{
        let mut buf: [u8; 8] = [0; 8];
        BigEndian::write_i64(&mut buf, value);
        return TimestampKey { 
            milliseconds: buf
        }
    }
}

impl db_key::Key for TimestampKey {
    fn from_u8(key: &[u8]) -> Self {
        let mut buf: [u8; 8] = [0; 8];
        buf.clone_from_slice(&key[..8]);
        return TimestampKey { milliseconds: buf};
    }

    fn as_slice<T, F: Fn(&[u8]) -> T>(&self, f: F) -> T {
        f(self.milliseconds.as_ref())
    }

}
impl std::convert::From<&[u8]> for TimestampKey {
    fn from(key: &[u8]) -> Self {
        let mut buf: [u8; 8] = [0; 8];
        buf.clone_from_slice(&key[..8]);
        return TimestampKey { milliseconds: buf};
    }
}

impl std::convert::AsRef<[u8]> for TimestampKey {
    fn as_ref(&self) -> &[u8] {
        return self.milliseconds.as_ref();
    }
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
fn insert_temperature_record(database_con: &mut leveldb::database::Database<TimestampKey>, env_record: EnvironmentalRecord) -> Result<(), String>
{
    let write_opts = WriteOptions::new();
    let serialized_data = match bincode::serialize(&env_record) {
        Ok(res) => res,
        Err(err) => {
            log::warn!(target: "dblog::db", "Could not serialize record into binary data: \'{}\'", err);
            return Err(String::from("Could not serialize record into binary data"));
        }
    };
    
    match database_con.put(write_opts, TimestampKey::from(env_record.timestamp),
        &serialized_data[..]) 
    {
        Ok(_) => {
            return Ok(());
        },
        Err(err) => {
            log::warn!(target: "dblog::db", "Could not insert record into database: \'{}\'", err);
            return Err(String::from("Could not insert record into database"));
        }
    }
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
    let database_path = std::path::Path::new(&connection_parameters.filename);

    let mut options = Options::new();
    options.create_if_missing = true;
    let mut database_con = match Database::<TimestampKey>::open(database_path, options) {
      Ok(db) => db,
      Err(err) => {
        log::error!(target: "dblogd::db", "Could not connect to database: \'{}\'", err);
        thread_finish.store(true, Ordering::SeqCst);
        return;
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

        match insert_temperature_record(&mut database_con, temperature_record) {
            Ok(_) => {}
            Err(err) => {
                log::error!(target: "dblogd::db", "Database insert failed: \'{}\'", err);
                continue;
            }
        }
    }
}
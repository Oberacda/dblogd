extern crate postgres;
extern crate chrono;
extern crate serde_json;
extern crate log;
extern crate log4rs;
extern crate ctrlc;
extern crate clap;


use std::sync::mpsc::{Sender, Receiver};
use std::sync::{mpsc, Arc};
use std::sync::atomic::{AtomicBool, Ordering};
use std::{thread, time, io};
use std::net::UdpSocket;

use serde::{Serialize, Deserialize};

use chrono::Utc;
use postgres::{Connection, TlsMode};
use std::thread::sleep;
use std::process::exit;

use clap::App;


#[derive(Serialize, Deserialize, Debug)]
struct TemperatureRecord {
    id: i64,
    timestamp: chrono::DateTime<Utc>,
    sensor_name: String,
    celsius: f64,
    humidity: f64
}

#[derive(Serialize, Deserialize, Debug)]
struct DatabaseParameters {
    hostname: String,
    port: u32,
    username: String,
    password: String,
    database: String
}

#[derive(Serialize, Deserialize, Debug)]
struct SocketParameters {
    address: String,
    port: u32,
}


fn socket_thread(tx: Sender<TemperatureRecord>, thread_finished: Arc<AtomicBool>, params: SocketParameters) {
    let socket: UdpSocket = match UdpSocket::bind(format!("{}:{}", params.address, params.port)) {
        Ok(socket) => socket,
        Err(err) => {
            log::error!(target: "dblogd::udp", "Could not open udp socket: \'{}\'", err);
            thread_finished.store(true, Ordering::SeqCst);
            return;
        }
    };
    match socket.set_nonblocking(true) {
        Ok(_) => log::debug!(target: "dblogd::udp", "Set socket to nonblocking mode!"),
        Err(err) => {
            log::error!(target: "dblogd::udp", "Could not set socket to nonblocking mode: \'{}\'", err);
            thread_finished.store(true, Ordering::SeqCst);
            return;
        }
    }

    match socket.local_addr() {
        Ok(res) => {
            log::info!(target: "dblogd::udp", "Socket Addr: \'{}\'", res);
        },
        Err(err) => {
            log::error!(target: "dblogd::udp", "Could not get socket address: \'{}\'", err);
            thread_finished.store(true, Ordering::SeqCst);
            return;
        }
    }

    let timeout = time::Duration::from_millis(100);

    while !thread_finished.load(Ordering::SeqCst) {
        // Receives a single datagram message on the socket. If `buf` is too small to hold
        // the message, it will be cut off.
        let mut buf: [u8; 1024]  = [0; 1024];

        let (buf_size, addr) = match socket.recv_from(&mut buf) {
            Ok(res) => res,
            Err(ref e) if e.kind() == io::ErrorKind::WouldBlock => {
                // wait until network socket is ready, typically implemented
                // via platform-specific APIs such as epoll or IOCP

                sleep(timeout);
                continue;
            },
            Err(msg) => {
                log::error!(target: "dblogd::udp", "Socket cannot recv data: \'{}\'", msg);
                continue
            }
        };

        log::debug!("Recieved data with length: \'{}\' from \'{}\'!", &buf_size, &addr);

        let recv_data_str = match std::str::from_utf8(&buf) {
            Ok(str) => str,
            Err(err) => {
                log::error!(target: "dblogd::udp", "Recieved data cannot be converted to UTF-8 str: \'{}\'", err);
                continue
            }
        };
        let recv_data_str_trimmed = match recv_data_str.trim_end().get(..buf_size) {
            Option::Some(res) => res,
            Option::None => {
                log::warn!(target: "dblogd::udp", "Received invalid packet!");
                continue;
            }
        };

        let json_buf_record = match serde_json::from_str::<TemperatureRecord>(recv_data_str_trimmed) {
            Ok(result) => result,
            Err(err) => {
                log::error!(target: "dblogd::udp", "Recieved data cannot be deserialized via JSON: \'{}\'", err);
                continue
            }
        };

        match tx.send(json_buf_record) {
            Ok(_) => log::debug!(target: "dblogd::udp", "Send message to database thread!"),
            Err(err) => {
                log::error!(target: "dblogd::udp", "Could not send message to database thread: \'{}\'", err);
            }
        };
    }
}

fn database_thread(rx: Receiver<TemperatureRecord>, thread_finished: Arc<AtomicBool>) {
    let database_connection: Connection = match Connection::connect("postgresql://u_home_client:temperature@raspberry3.local:5432/home_dev?application_name=dblogd", TlsMode::None)
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
                continue
            }
        };
        let probe_rows = match database_connection.query("INSERT INTO sensors.records (timestamp, sensor_name) VALUES ($1, $2) RETURNING id",
                                             &[&temperature_record.timestamp, &temperature_record.sensor_name]) {
            Ok(rows) => rows,
            Err(err) => {
                log::warn!(target: "dblog::db", "Could not insert probe into database: \'{}\'", err);
                continue
            }
        };

        let new_id: i64 = probe_rows.get(0).get("id");
        temperature_record.id = new_id;

        match database_connection.execute("INSERT INTO sensors.temperature (record_id, celsius) VALUES ($1, $2)",
                                          &[&temperature_record.id, &temperature_record.celsius]) {
            Ok(_) => {},
            Err(err) => {
                log::warn!(target: "dblog::db", "Could not insert celsius value into database: \'{}\'", err);
                continue
            }
        }
        match database_connection.execute("INSERT INTO sensors.humidity (record_id, humidity) VALUES ($1, $2)",
                                    &[&temperature_record.id, &temperature_record.humidity]) {
            Ok(_) => {},
            Err(err) => {
                log::warn!(target: "dblog::db", "Could not insert celsius value into database: \'{}\'", err);
                continue
            }
        }
    }
}

fn main() {
    let yaml = clap::load_yaml!("cli.yml");
    let matches = App::from(yaml).get_matches();
    if matches.is_present("config") {
        let _config = matches.value_of("config");
    }
    match log4rs::init_file("resources/log.yml", Default::default()) {
        Ok(_) => {},
        Err(err) => {
            log::error!("Could not create logger from yaml configuration: {}", err);
            exit(-100);
        }
    };

    let (tx, rx): (Sender<TemperatureRecord>, Receiver<TemperatureRecord>) = mpsc::channel();

    let terminate_programm = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let terminate_main_thread = Arc::clone(&terminate_programm);
    let terminate_socket_thread = Arc::clone(&terminate_programm);
    let terminate_database_thread = Arc::clone(&terminate_programm);

    let socket_parameters = SocketParameters {
        address: String::from("0.0.0.0"),
        port: 31454
    };

    let socket_thread = match thread::Builder::new()
        .name("socket".to_string())
        .spawn(move || {
            socket_thread(tx, terminate_socket_thread, socket_parameters);
        }) {
        Ok(socket_handle) => socket_handle,
        Err(err) => {
            log::error!(target: "dblogd", "Cannot start the udp socket thread: \'{}\'", err);
            exit(201);
        }
    };

    let database_thread = match thread::Builder::new()
        .name("database".to_string())
        .spawn(move || {
            database_thread(rx, terminate_database_thread);
        }) {
        Ok(socket_handle) => socket_handle,
        Err(err) => {
            log::error!(target: "dblogd", "Cannot start the database thread: \'{}\'", err);
            exit(202);
        }
    };

    ctrlc::set_handler(move || {
        log::info!(target: "dblogd","Termination signal received!");
        terminate_main_thread.store(true, Ordering::SeqCst);
    }).expect("Error setting Ctrl-C handler");

    match socket_thread.join() {
        Ok(_) => log::debug!(target: "dblogd", "Joined socket thread!"),
        Err(_) => {
            log::error!(target: "dblogd", "Could not join the socket thread!");
            exit(301);
        }
    };
    match database_thread.join() {
        Ok(_) => log::debug!(target: "dblogd", "Joined database thread!"),
        Err(_) => {
        log::error!(target: "dblogd", "Could not join the database thread!");
        exit(301);
        }
    };

    log::info!(target: "dblogd", "Exiting");
    exit(0);
}

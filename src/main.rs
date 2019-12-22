//! Crate to receive records from a tcp socket and insert them into a database.

extern crate chrono;
extern crate clap;
extern crate ctrlc;
extern crate log;
extern crate log4rs;
extern crate postgres;
extern crate serde_json;


use std::fs::File;
use std::io::Read;
use std::process::exit;
use std::sync::{Arc, mpsc};
use std::sync::atomic::Ordering;
use std::sync::mpsc::{Receiver, Sender};
use std::thread;

use clap::App;
use serde::{Deserialize, Serialize};

pub mod record;
mod socket;
mod database;

#[derive(Serialize, Deserialize, Debug, Clone)]
/// Struct representing the configuration of the application.
pub struct Configuration {
    /// Parameters for the database part ot the app.
    database_connection_parameters: database::DatabaseParameters,
    /// Parameters for the socket part of the app.
    socket_connection_parameters: socket::TlsSocketParameters,
}

/// Main function of the application.
///
/// It starts the database and socket threads.
/// This function will await a close command from the user or run indefinitely.
///
pub fn main() {
    let cli_yaml = clap::load_yaml!("cli.yml");
    let matches = App::from(cli_yaml).get_matches();
    if matches.is_present("config") {
        let _config = matches.value_of("config");
    }
    match log4rs::init_file("resources/log.yml", Default::default()) {
        Ok(_) => {}
        Err(err) => {
            log::error!("Could not create logger from yaml configuration: {}", err);
            exit(-100);
        }
    };

    let (tx, rx): (Sender<record::TemperatureRecord>, Receiver<record::TemperatureRecord>) = mpsc::channel();
    let socket_tx_channel = tx.clone();

    let terminate_programm = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let terminate_main_thread = Arc::clone(&terminate_programm);
    let terminate_socket_thread = Arc::clone(&terminate_programm);
    let terminate_database_thread = Arc::clone(&terminate_programm);

    let mut configuration_file = match File::open("resources/dblogd.yml") {
        Ok(file) => file,
        Err(err) => {
            log::error!(target: "dblogd", "Cannot open the configuration file: \'{}\'", err);
            return;
        }
    };

    let mut configuration_string = String::new();
    match configuration_file.read_to_string(&mut configuration_string) {
        Ok(_) => {}
        Err(err) => {
            log::error!(target: "dblogd", "Cannot read the configuration from file: \'{}\'", err);
            return;
        }
    };

    let configuration = match serde_yaml::from_str::<Configuration>(configuration_string.as_str()) {
        Ok(res) => res,
        Err(err) => {
            log::error!(target: "dblogd", "Cannot deserialize the configuration: \'{}\'", err);
            return;
        }
    };


    let socket_configuration = configuration.socket_connection_parameters.clone();
    let socket_thread = match thread::Builder::new()
        .name("socket".to_string())
        .spawn(move || {
            socket::thread_tcp_listener_socket(socket_tx_channel, terminate_socket_thread, socket_configuration);
        }) {
        Ok(socket_handle) => socket_handle,
        Err(err) => {
            log::error!(target: "dblogd", "Cannot start the udp socket thread: \'{}\'", err);
            exit(201);
        }
    };

    let database_configuration = configuration.database_connection_parameters.clone();
    let database_thread = match thread::Builder::new()
        .name("database".to_string())
        .spawn(move || {
            database::database_thread(rx, terminate_database_thread, database_configuration);
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

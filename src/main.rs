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
use log4rs::append::console::ConsoleAppender;
use log4rs::append::rolling_file::RollingFileAppender;
use log4rs::encode::pattern::PatternEncoder;
use log4rs::append::rolling_file::policy::compound::CompoundPolicy;
use log4rs::append::rolling_file::policy::compound::trigger::size::SizeTrigger;
use log4rs::append::rolling_file::policy::compound::roll::fixed_window::FixedWindowRoller;
use log4rs::config::{Config, Logger, Appender, Root};
use log::LevelFilter;
use std::path::Path;

pub mod record;
mod database;
mod mqtt_subscriber;

#[derive(Serialize, Deserialize, Debug, Clone)]
/// Struct representing the configuration of the application.
pub struct Configuration {
    /// Parameters for the database part ot the app.
    database_connection_parameters: database::DatabaseParameters,
    /// Parameters for the mqtt part of the app.
    mqtt_params: mqtt_subscriber::MqttParams,
    /// Logging folder location.
    logging_folder: String,
}

/// Main function of the application.
///
/// It starts the database and socket threads.
/// This function will await a close command from the user or run indefinitely.
///
pub fn main() {
    let cli_yaml = clap::load_yaml!("cli.yml");
    let matches = App::from(cli_yaml).get_matches();

    let config_file_path_str = match matches.value_of("config_file") {
        Some(config_file_path) => config_file_path,
        None => {
            println!("Requried argument \'config\' not found!");
            exit(104);
        }
    };
    let config_file_path = Path::new(config_file_path_str);
    let config_file_path_canon = match config_file_path.canonicalize() {
        Ok(path) => path,
        Err(err) => {
            println!("Cannot find the configuration path: \'{}\'", err);
            exit(105);
        }
    };

    let mut configuration_file = match File::open(config_file_path_canon) {
        Ok(file) => file,
        Err(err) => {
            println!("Cannot open the configuration file: \'{}\'", err);
            return;
        }
    };

    let mut configuration_string = String::new();
    match configuration_file.read_to_string(&mut configuration_string) {
        Ok(_) => {}
        Err(err) => {
            println!("Cannot read the configuration from file: \'{}\'", err);
            return;
        }
    };

    let configuration = match serde_yaml::from_str::<Configuration>(configuration_string.as_str()) {
        Ok(res) => res,
        Err(err) => {
            println!("Cannot deserialize the configuration: \'{}\'", err);
            return;
        }
    };

    let rolling_logger_file = format!("{}/dblogd.log", configuration.logging_folder);
    let rolling_logger_file_pattern = format!("{}/dblogd.{}.log", configuration.logging_folder, "{}");

    let stdout = ConsoleAppender::builder()
        .encoder(Box::new(PatternEncoder::new("{d(%Y-%m-%d %H:%M:%S %Z)(utc)} - {h({l})} - {t} - {T} - {m}{n}"))).build();
    let rolling_log_file = match RollingFileAppender::builder()
        .encoder(Box::new(PatternEncoder::new("{d(%Y-%m-%d %H:%M:%S %Z)(utc)} - {h({l})} - {t} - {T} - {m}{n}")))
        .build(
            rolling_logger_file.as_str(),
            Box::new(CompoundPolicy::new(
                Box::new(SizeTrigger::new(1000000)),
                Box::new(FixedWindowRoller::builder()
                    .base(1)
                    .build(rolling_logger_file_pattern.as_str(), 5).unwrap()
                    )
                )
            )
        ) {
        Ok(logger) => logger,
        Err(err) => {
            println!("Could not create rolling file logger: \'{}\'", err);
            exit(101);
        }
    };

    let log_config = match Config::builder()
        .appender(Appender::builder().build("stdout", Box::new(stdout)))
        .appender(Appender::builder().build("rolling_log_file", Box::new(rolling_log_file)))
        .logger(Logger::builder()
            .appenders(&[String::from("stdout"), String::from("rolling_log_file")])
            .additive(false)
            .build("dblogd::db", LevelFilter::Info))
        .logger(Logger::builder()
            .appenders(&[String::from("stdout"), String::from("rolling_log_file")])
            .additive(false)
            .build("dblogd::mqtt", LevelFilter::Info))
        .logger(Logger::builder()
            .appenders(&[String::from("stdout"), String::from("rolling_log_file")])
            .additive(false)
            .build("dblogd", LevelFilter::Info))
        .build(Root::builder()
            .appender("stdout")
            .build(LevelFilter::Warn)) {
        Ok(config) => config,
        Err(err) => {
            println!("{}", err);
            exit(102);
        }
    };

    match log4rs::init_config(log_config) {
        Ok(_) => {},
        Err(err) => {
            println!("Could not initialize logger: \'{}\'", err);
            exit(103);
        }
    };

    let (tx, rx): (Sender<record::EnvironmentalRecord>, Receiver<record::EnvironmentalRecord>) = mpsc::channel();
    let mqtt_tx_channel = tx.clone();

    let terminate_programm = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let terminate_main_thread = Arc::clone(&terminate_programm);
    let terminate_mqtt_thread = Arc::clone(&terminate_programm);
    let terminate_database_thread = Arc::clone(&terminate_programm);

    let mqtt_configuration = configuration.mqtt_params.clone();
    let mqtt_thread = match thread::Builder::new()
        .name("mqtt".to_string())
        .spawn(move || {
            mqtt_subscriber::thread_mqtt(mqtt_tx_channel, terminate_mqtt_thread, mqtt_configuration);
        }) {
        Ok(mqtt_handle) => mqtt_handle,
        Err(err) => {
            log::error!(target: "dblogd", "Cannot start the mqtt thread: \'{}\'", err);
            exit(201);
        }
    };

    let database_configuration = configuration.database_connection_parameters.clone();
    let database_thread = match thread::Builder::new()
        .name("database".to_string())
        .spawn(move || {
            database::database_thread(rx, terminate_database_thread, database_configuration);
        }) {
        Ok(database_thread) => database_thread,
        Err(err) => {
            log::error!(target: "dblogd", "Cannot start the database thread: \'{}\'", err);
            exit(202);
        }
    };

    ctrlc::set_handler(move || {
        log::info!(target: "dblogd","Termination signal received!");
        terminate_main_thread.store(true, Ordering::SeqCst);
    }).expect("Error setting Ctrl-C handler");

    match mqtt_thread.join() {
        Ok(_) => log::debug!(target: "dblogd", "Joined mqtt thread!"),
        Err(_) => {
            log::error!(target: "dblogd", "Could not join the mqtt thread!");
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

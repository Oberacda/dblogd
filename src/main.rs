extern crate postgres;
extern crate chrono;
extern crate serde_json;
extern crate log;
extern crate log4rs;

use std::sync::mpsc::{Sender, Receiver};
use std::sync::{mpsc, Arc};
use std::sync::atomic::{AtomicBool, Ordering};
use std::{thread, time, io};
use std::net::{UdpSocket, TcpStream};

use log::LevelFilter;
use log4rs::append::console::ConsoleAppender;
use log4rs::append::file::FileAppender;
use log4rs::encode::pattern::PatternEncoder;
use log4rs::config::{Appender, Config, Logger, Root};

use serde::{Serialize, Deserialize};
use serde_json::Deserializer;

use chrono::{DateTime, Utc, TimeZone};
use postgres::{Connection, TlsMode};
use std::str::Utf8Error;
use std::thread::sleep;


#[derive(Serialize, Deserialize, Debug)]
struct TemperatureRecord {
    id: i64,
    timestamp: chrono::DateTime<Utc>,
    sensor_name: String,
    celsius: f64,
    humidity: f64
}

fn socket_thread(tx: Sender<TemperatureRecord>, thread_finished: Arc<AtomicBool>) {
    let mut socket = UdpSocket::bind("127.0.0.1:34254").unwrap();
    socket.set_nonblocking(true).unwrap();

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
                log::error!(target: "dblogd::udp", "Socket cannot recv data: {}", msg);
                continue
            }
        };

        log::debug!("Recieved data with length: {} from {}!", &buf_size, &addr);

        let recv_data_str = match std::str::from_utf8(&buf) {
            Ok(str) => str,
            Err(err) => {
                log::error!(target: "dblogd::udp", "Recieved data cannot be converted to UTF-8 str: {}", err);
                continue
            }
        };
        let recv_data_str_trimmed = recv_data_str.trim_end();

        let json_buf_record = match serde_json::from_str::<TemperatureRecord>(recv_data_str) {
            Ok(result) => result,
            Err(err) => {
                log::error!(target: "dblogd::udp", "Recieved data cannot be deserialized via JSON: {}", err);
                continue
            }
        };

        tx.send(json_buf_record);
    }
}

fn database_thread(rx: Receiver<TemperatureRecord>, database_connection: Connection) {
    let mut temperature_record = rx.recv().unwrap();

    let rows = database_connection.query("INSERT INTO sensors.records (timestamp, sensor_name) VALUES ($1, $2) RETURNING id",
                          &[&temperature_record.timestamp, &temperature_record.sensor_name]).unwrap();
    let new_id: i64 = rows.get(0).get(0);
    temperature_record.id = new_id;

    database_connection.execute("INSERT INTO sensors.temperature (record_id, celsius) VALUES ($1, $2)",
                 &[&temperature_record.id, &temperature_record.celsius]).unwrap();
    database_connection.execute("INSERT INTO sensors.humidity (record_id, humidity) VALUES ($1, $2)",
                 &[&temperature_record.id, &temperature_record.humidity]).unwrap();

    for row in &database_connection.query("SELECT records.id, records.sensor_name, records.timestamp, \
    Temperature.celsius, humidity.humidity \
    FROM sensors.records \
    INNER JOIN sensors.humidity on humidity.record_id = records.id \
    INNER JOIN sensors.Temperature on Temperature.record_id = records.id", &[]).unwrap() {
        let record = TemperatureRecord {
            id: row.get("id"),
            timestamp: row.get("timestamp"),
            sensor_name: row.get("sensor_name"),
            celsius: row.get("celsius"),
            humidity: row.get("humidity"),
        };

        println!("Found temperature record {} with timestamp {} and values {}, {}", record.id , record.timestamp, record.celsius, record.humidity);
    }
}

fn main() {
    let stdout = ConsoleAppender::builder()
        .encoder(Box::new(PatternEncoder::new("{d(%Y-%m-%d %H:%M:%S %Z)(utc)} - {h({l})} - {M} - {T}  {m}{n}")))
        .build();

    let requests = FileAppender::builder()
        .encoder(Box::new(PatternEncoder::new("{d(%Y-%m-%d %H:%M:%S %Z)(utc)} - {h({l})} - {t} - {T} - {m}{n}")))
        .build("log/dblogd.log")
        .unwrap();

    let config = Config::builder()
        .appender(Appender::builder().build("stdout", Box::new(stdout)))
        .appender(Appender::builder().build("file", Box::new(requests)))
        .logger(Logger::builder().build("dblogd::udp", LevelFilter::Info))
        .logger(Logger::builder().build("dblogd::db", LevelFilter::Info))
        .build(Root::builder().appenders(vec! ["stdout", "file"]).build(LevelFilter::Warn))
        .unwrap();

    let handle = log4rs::init_config(config).unwrap();

    log::error!(target: "dblogd::udp", "Recieved data cannot be deserialized via JSON");

    let conn = Connection::connect("postgresql://home_user_default:temperature@localhost:5432/home-test-dev", TlsMode::None).unwrap();


    let (tx, rx): (Sender<TemperatureRecord>, Receiver<TemperatureRecord>) = mpsc::channel();
    let a = Arc::new(std::sync::atomic::AtomicBool::new(false));

    let a_c = Arc::clone(&a);
    let socket_thread = thread::spawn( move || {
        socket_thread(tx, a_c);
    });
    //let database_thread = thread::spawn(move || {
    //    database_thread(rx, conn);
    //});
    let ten_millis = time::Duration::from_secs(60);
    sleep(ten_millis);
    log::info!("Finished sleep!");

    a.store(true, Ordering::SeqCst);
    socket_thread.join();
    //database_thread.join();
}

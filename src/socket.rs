use std::{io, time};
use std::net::UdpSocket;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::mpsc::Sender;
use std::thread::sleep;

use serde::{Deserialize, Serialize};

use crate::record::TemperatureRecord;

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct SocketParameters {
    pub address: String,
    pub port: u32,
}

pub fn socket_thread(tx: Sender<TemperatureRecord>, thread_finished: Arc<AtomicBool>, params: SocketParameters) {
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
        }
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
        let mut buf: [u8; 1024] = [0; 1024];

        let (buf_size, addr) = match socket.recv_from(&mut buf) {
            Ok(res) => res,
            Err(ref e) if e.kind() == io::ErrorKind::WouldBlock => {
                // wait until network socket is ready, typically implemented
                // via platform-specific APIs such as epoll or IOCP

                sleep(timeout);
                continue;
            }
            Err(msg) => {
                log::error!(target: "dblogd::udp", "Socket cannot recv data: \'{}\'", msg);
                continue;
            }
        };

        log::debug!("Recieved data with length: \'{}\' from \'{}\'!", &buf_size, &addr);

        let recv_data_str = match std::str::from_utf8(&buf) {
            Ok(str) => str,
            Err(err) => {
                log::error!(target: "dblogd::udp", "Recieved data cannot be converted to UTF-8 str: \'{}\'", err);
                continue;
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
                continue;
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
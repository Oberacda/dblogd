//!
//! Module to manage a TCP/TLS socket that passes valid json TemperatureRecords payloads from the
//! socket to the database thread.
//!
use std::{io, time};
use std::fs::File;
use std::io::Read;
use std::net::{TcpListener, TcpStream};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::mpsc::Sender;

use native_tls::{HandshakeError, Identity, MidHandshakeTlsStream, Protocol, TlsAcceptor, TlsStream};
use serde::{Deserialize, Serialize};
use threadpool::ThreadPool;

use crate::record::TemperatureRecord;

#[derive(Serialize, Deserialize, Debug, Clone)]
/// Struct representing the parameters needed for establishing a simple UDP or TCP socket.
pub struct SocketParameters
{
    /// The ip address the socket should listen on.
    pub address: String,
    /// The port the socket should listen on.
    pub port: u32,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
/// Struct representing the parameters for establishing a TCP/TLS socket.
///
/// This socket is encrypted with a pkcs12 certificate/key file.
pub struct TlsSocketParameters
{
    /// The prarameters for establishing a socket.
    pub socket_params: SocketParameters,
    /// The location of the pkcs12 cert/key file.
    pub pkcs12_identity_file: String,
    /// The password to unlock the encrypted key pair.
    pub pkcs12_file_password: String,
}

///
/// Function handling a single tcp/tls data stream to a remote client.
///
/// Valid json data received by this thread is moved to the database thread.
/// This thread will never block for more than 100ms.
///
/// Received packages can not be longer than 512 bytes.
/// Iff they are longer they will be capped.
///
/// # Arguments
///
/// * `stream` - The TCP/Tls stream to communicate with the remote peer.
///
/// * `tx` - Sender to transfer the valid data received from the remote host to the database thread.
///
/// * `thread_finish` - Thread shared boolean to indicate if the thread should finish running.
///
/// # Errors
///
/// Errors occur when one of the following conditions is met:
///
/// * When the stream can not be set the read method into the nonblocking mode.
///
/// * When the socket connection cannot be terminated.
///
/// These errors will result in the method immediately exiting without raising a exception.
///
fn handle_tls_stream(
    mut stream: TlsStream<TcpStream>,
    tx: Sender<TemperatureRecord>,
    thread_finish: Arc<AtomicBool>)
{

    match stream.get_mut().set_read_timeout(Some(time::Duration::from_millis(100))) {
        Ok(_) => {}
        Err(err) => {
            log::error!(target: "dblogd::socket::tls", "Unable to set connection nonblocking: \'{}\'", err);
            match stream.shutdown() {
                Ok(_) => {}
                Err(err) => {
                    log::error!(target: "dblogd::socket::tls", "Unable to close tls connection: \'{}\'", err);
                }
            };
            return;
        }
    };

    while !thread_finish.load(Ordering::SeqCst) {

        let mut recv_vec: [u8; 512] = [0; 512];
        let recv_bytes_read = match stream.read(&mut recv_vec) {
            Ok(0) => {
                log::debug!(target: "dblogd::socket::tls", "Socket connection closed!");
                break;
            }
            Ok(bytes_read) => bytes_read,
            Err(ref e) if e.kind() == io::ErrorKind::WouldBlock => {
                // wait until network socket is ready, typically implemented
                // via platform-specific APIs such as epoll or IOCP
                continue;
            }
            Err(err) => {
                log::error!(target: "dblogd::socket::tls", "Socket cannot read data: \'{}\'", err);
                continue;
            }
        };

        let recv_string = match std::str::from_utf8(&recv_vec[..recv_bytes_read]) {
            Ok(string) => String::from(string),
            Err(err) => {
                log::warn!(target: "dblogd::socket::tls", "Socket received non UTF-8 data: \'{}\'", err);
                continue;
            }
        };

        let recv_data_str_trimmed = recv_string.trim_end();

        let json_buf_record = match serde_json::from_str::<TemperatureRecord>(recv_data_str_trimmed) {
            Ok(result) => result,
            Err(err) => {
                log::error!(target: "dblogd::socket::tls", "Recieved data cannot be deserialized via JSON: \'{}\'", err);
                continue;
            }
        };

        match tx.send(json_buf_record) {
            Ok(_) => log::debug!(target: "dblogd::socket::tls", "Send message to database thread!"),
            Err(err) => {
                log::error!(target: "dblogd::socket::tls", "Could not send message to database thread: \'{}\'", err);
            }
        };
    }
    match stream.shutdown() {
        Ok(_) => {}
        Err(err) => {
            log::error!(target: "dblogd::socket::tls", "Unable to close tls connection: \'{}\'", err);
        }
    };
}

/// Function to perform a tls handshake if the Tls stream has been interrupted in a nonblocking mode.
///
/// This allows to finish a intterupted tls handshake.
///
/// **Warning:** This function will retry this operation for a infinite time if the operation keeps
/// getting interrupted. There is no guarantee this will not result in a stack overflow.
///
/// # Arguments
///
/// * `incomplete_handshake_stream` - The interrupted stream. The function will try to establish a
///     handshake on this stream.
/// * `stream` - Optional either containing the established stream or non if no stream could be established.
///     This is a **output** parameter.
///
/// # Future
///
/// This function will probably be reworked in the future as it is suboptimal is every way.
///
fn tls_handshake(incomplete_handshake_stream: MidHandshakeTlsStream<TcpStream>, stream: &mut Option<TlsStream<TcpStream>>)
{
    match incomplete_handshake_stream.handshake() {
        Ok(tls_stream) => {
            *stream = Some(tls_stream);
            return;
        }
        Err(err) => match err {
            HandshakeError::WouldBlock(handshake_conn) => {
                tls_handshake(handshake_conn, stream);
                return;
            }
            HandshakeError::Failure(err) => {
                log::error!(target: "dblogd::socket", "Could not perform tls handshake: \'{}\'", err);
                return;
            }
        }
    };
}

/// Thread function for the socket functions.
///
/// This function accepts incoming connections and allows them to send encrypted json data that will
/// be relayed to the database thread.
///
/// This function will run until the `thread_finish` parameter was set or the socket is closed by a error.
///
/// # Arguments
///
/// * `tx` - Sender that is used to pass valid data to the database thread.
///
/// * `thread_finish` - Indicates that the thread should finish operation and should return.
///
/// * `params` - Parameters for the socket and the tls connection.
///
/// # Errors
///
/// Errors occur when one of the following conditions is met:
///
/// * The identity for the TLS connection cannot be found.
///
/// * The socket cannot be created or listened to.
///
/// * The socket cannot be set to nonblocking mode.
///
/// These errors will result in the method immediately exiting without raising a exception.
///
pub fn thread_tcp_listener_socket(tx: Sender<TemperatureRecord>, thread_finish: Arc<AtomicBool>, params: TlsSocketParameters)
{
    let mut pkcs12_identity_file = match File::open(params.pkcs12_identity_file) {
        Ok(file) => file,
        Err(err) => {
            log::error!(target: "dblogd::socket", "Could not open pkcs12 identity: \'{}\'", err);
            thread_finish.store(true, Ordering::SeqCst);
            return;
        }
    };
    let mut pkcs12_identity = vec![];
    match pkcs12_identity_file.read_to_end(&mut pkcs12_identity) {
        Ok(_) => {}
        Err(err) => {
            log::error!(target: "dblogd::socket", "Could not read pkcs12 identity: \'{}\'", err);
            thread_finish.store(true, Ordering::SeqCst);
            return;
        }
    };

    let identity = match Identity::from_pkcs12(&pkcs12_identity, params.pkcs12_file_password.as_str()) {
        Ok(idn) => idn,
        Err(err) => {
            log::error!(target: "dblogd::socket", "Could create identity from pkcs12: \'{}\'", err);
            thread_finish.store(true, Ordering::SeqCst);
            return;
        }
    };

    let mut tls_acceptor_builder = TlsAcceptor::builder(identity);
    tls_acceptor_builder.min_protocol_version(Some(Protocol::Tlsv12));


    let tls_acceptor = match tls_acceptor_builder.build() {
        Ok(acc) => acc,
        Err(err) => {
            log::error!(target: "dblogd::socket", "Could create tls acceptor from identity: \'{}\'", err);
            thread_finish.store(true, Ordering::SeqCst);
            return;
        }
    };

    let tcp_listener = match TcpListener::bind(format!("{}:{}", params.socket_params.address, params.socket_params.port)) {
        Ok(listener) => listener,
        Err(err) => {
            log::error!(target: "dblogd::socket", "Could not open tcp listener: \'{}\'", err);
            thread_finish.store(true, Ordering::SeqCst);
            return;
        }
    };
    tcp_listener.set_nonblocking(true).expect("Cannot set non-blocking");
    match tcp_listener.local_addr() {
        Ok(res) => {
            log::info!(target: "dblogd::socket", "Socket Addr: \'{}\'", res);
        }
        Err(err) => {
            log::error!(target: "dblogd::socket", "Could not get socket address: \'{}\'", err);
            thread_finish.store(true, Ordering::SeqCst);
            return;
        }
    }


    let thread_pool = ThreadPool::with_name(String::from("tls_threads"), 10);

    for stream in tcp_listener.incoming() {
        if thread_finish.load(Ordering::SeqCst) {
            thread_pool.join();
            return;
        }

        match stream {
            Ok(stream) => {
                let tls_acceptor = tls_acceptor.clone();
                let finish_connection_thread = Arc::clone(&thread_finish);
                let tx_connection = tx.clone();

                thread_pool.execute(move || {
                    let tls_stream = match tls_acceptor.accept::<TcpStream>(stream) {
                        Ok(stream) => stream,
                        Err(err) => {
                            match err {
                                HandshakeError::WouldBlock(handshake_conn) => {
                                    let mut stream = Option::<TlsStream<TcpStream>>::None;
                                    tls_handshake(handshake_conn, &mut stream);
                                    match stream {
                                        Some(tls_stream) => tls_stream,
                                        None => {
                                            log::error!(target: "dblogd::socket", "Could not perform tls handshake!");
                                            return;
                                        }
                                    }
                                }
                                HandshakeError::Failure(err) => {
                                    log::error!(target: "dblogd::socket", "Could not perform tls handshake: \'{}\'", err);
                                    return;
                                }
                            }
                        }
                    };
                    match tls_stream.get_ref().peer_addr() {
                        Ok(addr) => {
                            log::debug!(target: "dblogd::socket", "Connected to {}:{}", addr.ip(), addr.port());
                        }
                        Err(err) => {
                            log::warn!(target: "dblogd::socket", "Could not get connection address: \'{}\'", err);
                        }
                    };

                    handle_tls_stream(tls_stream, tx_connection, finish_connection_thread);
                });
            }
            Err(ref err) if err.kind() == io::ErrorKind::WouldBlock => {
                // wait until network socket is ready, typically implemented
                // via platform-specific APIs such as epoll or IOCP
                continue;
            }
            Err(err) => {
                log::error!(target: "dblogd::socket", "Could not connect to tcp stream: \'{}\'", err);
                continue;
            }
        }
    }
}

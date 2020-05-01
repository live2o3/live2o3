use rml_rtmp::handshake::{Handshake, HandshakeProcessResult, PeerType};
use slab::Slab;
use std::collections::HashMap;
use std::error::Error;
use std::sync::Arc;
use tokio::net::tcp::{OwnedReadHalf, OwnedWriteHalf};
use tokio::net::{TcpListener, TcpStream};
use tokio::prelude::*;
use tokio::sync::mpsc::{unbounded_channel, UnboundedReceiver, UnboundedSender};
use tokio::sync::Mutex;

mod server;
use server::{Server, ServerResult};

type Exception = Box<dyn Error + Sync + Send + 'static>;

enum Response {
    Close,
    Data(Vec<u8>),
}

struct ResponseSenderMap(HashMap<usize, UnboundedSender<Response>>, Slab<()>);

enum Request {
    Data(usize, Vec<u8>),
    Close(usize),
}

const BUFFER_SIZE: usize = 4096;

struct CloseGuard(usize, UnboundedSender<Request>);

impl Drop for CloseGuard {
    fn drop(&mut self) {
        match self.1.send(Request::Close(self.0)) {
            Ok(_) => {}
            Err(_) => {
                eprintln!("failed to send ServerMessage::Close({}) to server", self.0);
            }
        }
    }
}

///
/// 共有四个循环：
///
/// 1. （main） 监听入站连接循环，接收到一个 `TcpStream` 对象之后，异步调用 `handle_connection` 函数处理之
/// 2. （connection_read_loop） `TcpStream` 读取循环，循环读取 `TcpStream`，使用接收到的数据构造 `ServerMessage` 发送给 3.
/// 3. （server_loop) RTMP 服务器循环，接收到 2. 发送过来的 `ServerMessage` 对象，并处理之，将服务器处理的结果构造为 `Response` 并发送给 4.
/// 4. （connection_write_loop) `TcpStream` 写入循环，接收到 3. 发送过来的 `Response` 对象并写入到 `TcpStream` 对象中，将响应返回给客户端
///
/// client -> 2.:   [client] -> tcp_reader
/// 2. -> 3. :      request_sender -> request_receiver
/// 3. -> 4. :      response_sender -> response_receiver
/// 4. -> client:   tcp_writer -> [client]
///
#[tokio::main]
async fn main() -> Result<(), Exception> {
    let address = "0.0.0.0:1935";
    let mut listener = TcpListener::bind(address).await?;

    let (request_sender, request_receiver) = unbounded_channel();
    let response_sender_map: Arc<Mutex<ResponseSenderMap>> =
        Arc::new(Mutex::new(ResponseSenderMap(
            HashMap::<usize, UnboundedSender<Response>>::new(),
            Slab::<()>::new(),
        )));
    tokio::spawn(server_loop(request_receiver, response_sender_map.clone()));

    println!("Listening for connections on {}", address);
    while let Ok((stream, addr)) = listener.accept().await {
        println!("{} connected.", addr);

        tokio::spawn(handle_connection(
            stream,
            response_sender_map.clone(),
            request_sender.clone(),
        ));
    }

    Ok(())
}

#[derive(Debug)]
pub enum ConnectionError {
    IoError(std::io::Error),
    SocketClosed,
    HandshakeError(rml_rtmp::handshake::HandshakeError),
}

impl From<std::io::Error> for ConnectionError {
    fn from(e: std::io::Error) -> Self {
        ConnectionError::IoError(e)
    }
}

impl From<rml_rtmp::handshake::HandshakeError> for ConnectionError {
    fn from(e: rml_rtmp::handshake::HandshakeError) -> Self {
        ConnectionError::HandshakeError(e)
    }
}

async fn handshake(stream: &mut TcpStream) -> Result<Vec<u8>, ConnectionError> {
    let mut handshake = Handshake::new(PeerType::Server);

    let mut buffer = [0u8; BUFFER_SIZE];
    while let Ok(len) = stream.read(&mut buffer).await {
        if len == 0 {
            // EOF
            return Err(ConnectionError::SocketClosed);
        }

        let result = handshake.process_bytes(&buffer[..len])?;

        match result {
            HandshakeProcessResult::InProgress { response_bytes } => {
                stream.write(response_bytes.as_slice()).await?;
            }
            HandshakeProcessResult::Completed {
                response_bytes,
                remaining_bytes,
            } => {
                stream.write(response_bytes.as_slice()).await?;
                return Ok(remaining_bytes);
            }
        }
    }

    Err(ConnectionError::SocketClosed)
}

async fn handle_connection(
    mut stream: TcpStream,
    sender_map: Arc<Mutex<ResponseSenderMap>>,
    request_sender: UnboundedSender<Request>,
) {
    let (response_sender, response_receiver) = unbounded_channel();
    let id = {
        let mut guard = sender_map.lock().await;
        let id = guard.1.insert(());
        guard.0.insert(id, response_sender);
        id
    };

    let remaining_bytes = match handshake(&mut stream).await {
        Ok(remaining_bytes) => remaining_bytes,
        Err(e) => {
            eprintln!("handshake error: {:?}", e);
            return;
        }
    };

    let (tcp_reader, tcp_writer) = stream.into_split();
    tokio::spawn(connection_write_loop(id, response_receiver, tcp_writer));
    tokio::spawn(connection_read_loop(
        id,
        request_sender,
        tcp_reader,
        remaining_bytes,
    ));
}

async fn connection_write_loop(
    id: usize,
    mut response_receiver: UnboundedReceiver<Response>,
    mut tcp_writer: OwnedWriteHalf,
) {
    while let Some(packet) = response_receiver.recv().await {
        match packet {
            Response::Close => {
                println!("connection {} closed.", id);
                return;
            }
            Response::Data(buffer) => match tcp_writer.write(buffer.as_slice()).await {
                Ok(_len) => {}
                Err(e) => {
                    eprintln!("write buffer to connection {} error: {}", id, e);
                    return;
                }
            },
        }
    }
}

async fn connection_read_loop(
    id: usize,
    request_sender: UnboundedSender<Request>,
    mut tcp_reader: OwnedReadHalf,
    remaining_bytes: Vec<u8>,
) {
    let _close_guard = CloseGuard(id, request_sender.clone());

    if !remaining_bytes.is_empty() {
        match request_sender.send(Request::Data(id, remaining_bytes)) {
            Ok(_) => {}
            Err(_) => {
                eprintln!("connection {} send buffer to server error", id);
                return;
            }
        }
    }

    let mut buffer = [0; BUFFER_SIZE];
    loop {
        match tcp_reader.read(&mut buffer).await {
            Ok(len) => {
                if len == 0 {
                    eprintln!("connection {} got EOF", id);
                    return;
                }

                match request_sender.send(Request::Data(id, Vec::from(&buffer[..len]))) {
                    Ok(_) => {}
                    Err(_) => {
                        eprintln!("connection {} send buffer to server error", id);
                        return;
                    }
                }
            }
            Err(e) => {
                eprintln!("connection {} read error: {}", id, e);
                return;
            }
        }
    }
}

async fn server_loop(
    mut request_receiver: UnboundedReceiver<Request>,
    response_sender_map: Arc<Mutex<ResponseSenderMap>>,
) {
    let mut server = Server::new();

    while let Some(message) = request_receiver.recv().await {
        match message {
            Request::Data(connection_id, buffer) => {
                match server.bytes_received(connection_id, buffer.as_slice()) {
                    Ok(mut result) => {
                        for result in result.drain(..) {
                            match result {
                                ServerResult::DisconnectConnection { connection_id } => {
                                    let mut guard = response_sender_map.lock().await;
                                    match guard.0.get_mut(&connection_id) {
                                        Some(sender) => match sender.send(Response::Close) {
                                            Ok(_) => (),
                                            Err(_e) => eprintln!(
                                                "send Close packet to {} error",
                                                connection_id
                                            ),
                                        },
                                        None => eprintln!(
                                            "cannot find packet sender of {}",
                                            connection_id
                                        ),
                                    }
                                }
                                ServerResult::OutboundPacket {
                                    target_connection_id,
                                    packet,
                                } => {
                                    let mut guard = response_sender_map.lock().await;
                                    match guard.0.get_mut(&target_connection_id) {
                                        Some(sender) => {
                                            match sender.send(Response::Data(packet.bytes)) {
                                                Ok(_) => (),
                                                Err(_e) => eprintln!(
                                                    "send Response packet to {} error",
                                                    target_connection_id
                                                ),
                                            }
                                        }
                                        None => eprintln!(
                                            "cannot find packet sender of {}",
                                            connection_id
                                        ),
                                    }
                                }
                            }
                        }
                    }
                    Err(error) => {
                        eprintln!("server error occurred: {}", error);
                        // send close packet to connection {connection_id}
                        let mut guard = response_sender_map.lock().await;
                        match guard.0.get_mut(&connection_id) {
                            Some(sender) => match sender.send(Response::Close) {
                                Ok(_) => (),
                                Err(_) => eprintln!("send Close packet to {} error", connection_id),
                            },
                            None => eprintln!("cannot find packet sender of {}", connection_id),
                        }
                    }
                }
            }
            Request::Close(connection_id) => {
                server.notify_connection_closed(connection_id);
                let mut guard = response_sender_map.lock().await;
                guard.0.remove(&connection_id);
                guard.1.remove(connection_id);
            }
        }
    }
}

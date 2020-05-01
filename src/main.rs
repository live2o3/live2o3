use slab::Slab;
use std::error::Error;
use tokio::net::{TcpListener, TcpStream};
use tokio::prelude::*;
use tokio::sync::mpsc::{unbounded_channel, UnboundedReceiver, UnboundedSender};

use std::collections::HashMap;

mod server;
use crate::server::ServerResult;
use rml_rtmp::handshake::{Handshake, HandshakeProcessResult, PeerType};
use server::Server;
use std::sync::Arc;
use tokio::net::tcp::{OwnedReadHalf, OwnedWriteHalf};
use tokio::sync::Mutex;

type Exception = Box<dyn Error + Sync + Send + 'static>;

enum PacketToSend {
    Close,
    Response(Vec<u8>),
}

struct SenderMap(HashMap<usize, UnboundedSender<PacketToSend>>, Slab<()>);

type ArcSenderMap = Arc<Mutex<SenderMap>>;

enum ServerMessage {
    Data(usize, Vec<u8>),
    Close(usize),
}

const BUFFER_SIZE: usize = 4096;

struct CloseGuard(usize, UnboundedSender<ServerMessage>);

impl Drop for CloseGuard {
    fn drop(&mut self) {
        match self.1.send(ServerMessage::Close(self.0)) {
            Ok(_) => {}
            Err(_) => {
                eprintln!("failed to send ServerMessage::Close({}) to server", self.0);
            }
        }
    }
}

#[tokio::main]
async fn main() -> Result<(), Exception> {
    let address = "0.0.0.0:1935";
    let mut listener = TcpListener::bind(address).await?;

    let (buffer_sender, buffer_receiver) = unbounded_channel();
    let packet_sender_map: ArcSenderMap = Arc::new(Mutex::new(SenderMap(
        HashMap::<usize, UnboundedSender<PacketToSend>>::new(),
        Slab::<()>::new(),
    )));
    tokio::spawn(server_loop(buffer_receiver, packet_sender_map.clone()));

    println!("Listening for connections on {}", address);
    while let Ok((stream, addr)) = listener.accept().await {
        println!("{} connected.", addr);

        tokio::spawn(connection_loop(
            stream,
            packet_sender_map.clone(),
            buffer_sender.clone(),
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

async fn connection_loop(
    mut stream: TcpStream,
    sender_map: ArcSenderMap,
    buffer_sender: UnboundedSender<ServerMessage>,
) {
    let (packet_sender, packet_receiver) = unbounded_channel();
    let id = {
        let mut guard = sender_map.lock().await;
        let id = guard.1.insert(());
        guard.0.insert(id, packet_sender);
        id
    };

    let remaining_bytes = match handshake(&mut stream).await {
        Ok(remaining_bytes) => remaining_bytes,
        Err(e) => {
            eprintln!("handshake error: {:?}", e);
            return;
        }
    };

    let (reader, writer) = stream.into_split();
    tokio::spawn(connection_write_loop(id, packet_receiver, writer));
    tokio::spawn(connection_read_loop(
        id,
        buffer_sender,
        reader,
        remaining_bytes,
    ));
}

async fn connection_write_loop(
    id: usize,
    mut packet_receiver: UnboundedReceiver<PacketToSend>,
    mut writer: OwnedWriteHalf,
) {
    while let Some(packet) = packet_receiver.recv().await {
        match packet {
            PacketToSend::Close => {
                println!("connection {} closed.", id);
                return;
            }
            PacketToSend::Response(buffer) => match writer.write(buffer.as_slice()).await {
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
    buffer_sender: UnboundedSender<ServerMessage>,
    mut reader: OwnedReadHalf,
    remaining_bytes: Vec<u8>,
) {
    let _close_guard = CloseGuard(id, buffer_sender.clone());

    if remaining_bytes.len() > 0 {
        match buffer_sender.send(ServerMessage::Data(id, remaining_bytes)) {
            Ok(_) => {}
            Err(_) => {
                eprintln!("connection {} send buffer to server error", id);
                return;
            }
        }
    }

    let mut buffer = [0; BUFFER_SIZE];
    loop {
        match reader.read(&mut buffer).await {
            Ok(len) => {
                if len == 0 {
                    eprintln!("connection {} got EOF", id);
                    return;
                }

                match buffer_sender.send(ServerMessage::Data(id, Vec::from(&buffer[..len]))) {
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
    mut buffer_receiver: UnboundedReceiver<ServerMessage>,
    sender_map: ArcSenderMap,
) {
    let mut server = Server::new();

    while let Some(message) = buffer_receiver.recv().await {
        match message {
            ServerMessage::Data(connection_id, buffer) => {
                match server.bytes_received(connection_id, buffer.as_slice()) {
                    Ok(mut result) => {
                        for result in result.drain(..) {
                            match result {
                                ServerResult::DisconnectConnection { connection_id } => {
                                    let mut guard = sender_map.lock().await;
                                    match guard.0.get_mut(&connection_id) {
                                        Some(sender) => match sender.send(PacketToSend::Close) {
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
                                    let mut guard = sender_map.lock().await;
                                    match guard.0.get_mut(&target_connection_id) {
                                        Some(sender) => {
                                            match sender.send(PacketToSend::Response(packet.bytes))
                                            {
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
                        let mut guard = sender_map.lock().await;
                        match guard.0.get_mut(&connection_id) {
                            Some(sender) => match sender.send(PacketToSend::Close) {
                                Ok(_) => (),
                                Err(_) => eprintln!("send Close packet to {} error", connection_id),
                            },
                            None => eprintln!("cannot find packet sender of {}", connection_id),
                        }
                    }
                }
            }
            ServerMessage::Close(connection_id) => {
                server.notify_connection_closed(connection_id);
                let mut guard = sender_map.lock().await;
                guard.0.remove(&connection_id);
                guard.1.remove(connection_id);
            }
        }
    }
}

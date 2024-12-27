use std::net::SocketAddr;
use tokio::sync::mpsc;
use tokio_stream::{wrappers::ReceiverStream, StreamExt};
use tonic::async_trait;
use tonic::{transport::Server, Request, Response, Status};

pub mod services {
    tonic::include_proto!("packet");
}

use services::packet_service_server::{PacketService, PacketServiceServer};
use services::{Packet, PacketResponse};

pub struct PacketServiceImpl {
    conn: libsql::Connection,
}

impl PacketServiceImpl {
    pub fn new(conn: libsql::Connection) -> Self {
        PacketServiceImpl { conn }
    }
}

#[async_trait]
impl PacketService for PacketServiceImpl {
    type StreamPacketStream = ReceiverStream<Result<PacketResponse, Status>>;

    async fn stream_packet(
        &self,
        request: Request<tonic::Streaming<Packet>>,
    ) -> Result<Response<Self::StreamPacketStream>, Status> {
        println!(
            "Received a streaming request from {:?}",
            request.remote_addr()
        );

        let (tx, rx) = mpsc::channel(4);
        let mut packet_stream = request.into_inner();
        let conn = self.conn.clone();

        tokio::spawn(async move {
            while let Some(packet) = packet_stream.next().await {
                match packet {
                    Ok(packet) => {
                        let result = conn.execute(
                            "INSERT INTO packets (packet_id, source_ip, destination_ip, source_port, destination_port, payload, timestamp, protocol, packet_size, payload_entropy) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10)",
                            libsql::params![
                                packet.packet_id.clone(),
                                packet.source_ip.clone(),
                                packet.destination_ip.clone(),
                                packet.source_port,
                                packet.destination_port,
                                packet.payload,
                                packet.timestamp,
                                packet.protocol,
                                packet.packet_size,
                                packet.payload_entropy
                            ],
                        ).await;

                        if let Err(e) = result {
                            eprintln!("Error receiving packet: {}", e);
                            break;
                        };

                        let response = PacketResponse {
                            packet_id: packet.packet_id.clone(),
                            message: format!(
                                "Processed packet from {} to {}",
                                packet.source_ip, packet.destination_ip
                            ),
                            timestamp: packet.timestamp,
                        };

                        if let Err(e) = tx.send(Ok(response)).await {
                            eprintln!("Failed to send response: {}", e);
                            break;
                        }
                    }
                    Err(e) => {
                        eprintln!("Error receiving packet: {}", e);
                        break;
                    }
                }
            }
        });
        Ok(Response::new(ReceiverStream::new(rx)))
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let addr: SocketAddr = "[::1]:80".parse().unwrap();
    let url = std::env::var("DATABASE_URL").unwrap();
    let auth_token = std::env::var("DATABASE_AUTH_TOKEN").unwrap_or_else(|_| {
        println!("TURSO_AUTH_TOKEN not set, using empty token...");
        "".to_string()
    });

    let db = libsql::Builder::new_remote(url, auth_token)
        .build()
        .await
        .unwrap();

    let conn = db.connect().unwrap();
    migrate(&conn).await.unwrap();

    let service = PacketServiceImpl::new(conn);

    println!("Server listening on {}", addr);

    Server::builder()
        .add_service(PacketServiceServer::new(service))
        .serve(addr)
        .await?;

    Ok(())
}

async fn migrate(conn: &libsql::Connection) -> Result<(), Box<dyn std::error::Error>> {
    conn.execute(
        "CREATE TABLE IF NOT EXISTS packets (
    packet_id TEXT PRIMARY KEY,
    source_ip TEXT,
    destination_ip TEXT,
    source_port INTEGER,
    destination_port INTEGER,
    payload BLOB,
    timestamp INTEGER,
    protocol TEXT,
    packet_size INTEGER,
    payload_entropy REAL
);",
        (),
    )
    .await
    .unwrap();
    println!("Success migrate");

    Ok(())
}

// TODO Logging

mod util;

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use anyhow::{anyhow, bail};
use cove_core::conn::{self, ConnMaintenance, ConnRx, ConnTx};
use cove_core::packets::{
    Cmd, IdentifyCmd, IdentifyRpl, JoinNtf, NickCmd, NickNtf, NickRpl, Packet, PartNtf, RoomCmd,
    RoomRpl, SendCmd, SendNtf, SendRpl, WhoCmd, WhoRpl,
};
use cove_core::{Identity, Message, MessageId, Session, SessionId};
use log::{info, warn};
use rand::Rng;
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::Mutex;
use tokio_tungstenite::MaybeTlsStream;

#[derive(Debug, Clone)]
struct Client {
    session: Session,
    send: ConnTx,
}

#[derive(Debug)]
struct Room {
    name: String,
    clients: HashMap<SessionId, Client>,
    last_message: MessageId,
    last_timestamp: u128,
}

impl Room {
    fn new(name: String) -> Self {
        Self {
            name,
            clients: HashMap::new(),
            last_message: MessageId::of(&format!("{}", rand::thread_rng().gen::<u64>())),
            last_timestamp: util::timestamp(),
        }
    }

    fn client(&self, id: SessionId) -> &Client {
        self.clients.get(&id).expect("invalid session id")
    }

    fn client_mut(&mut self, id: SessionId) -> &mut Client {
        self.clients.get_mut(&id).expect("invalid session id")
    }

    fn notify_all(&self, packet: &Packet) {
        for client in self.clients.values() {
            let _ = client.send.send(packet);
        }
    }

    fn notify_except(&self, id: SessionId, packet: &Packet) {
        for client in self.clients.values() {
            if client.session.id != id {
                let _ = client.send.send(packet);
            }
        }
    }

    fn join(&mut self, client: Client) {
        if self.clients.contains_key(&client.session.id) {
            // Session ids are generated randomly and a collision should be very
            // unlikely.
            panic!("duplicated session id");
        }

        self.notify_all(&Packet::ntf(JoinNtf {
            who: client.session.clone(),
        }));

        self.clients.insert(client.session.id, client);
    }

    fn part(&mut self, id: SessionId) {
        let client = self.clients.remove(&id).expect("invalid session id");

        self.notify_all(&Packet::ntf(PartNtf {
            who: client.session,
        }));
    }

    fn nick(&mut self, id: SessionId, nick: String) {
        let who = {
            let client = self.client_mut(id);
            client.session.nick = nick;
            client.session.clone()
        };

        self.notify_except(id, &Packet::ntf(NickNtf { who }))
    }

    fn send(&mut self, id: SessionId, parent: Option<MessageId>, content: String) -> Message {
        let client = &self.clients[&id];

        self.last_timestamp = util::timestamp_after(self.last_timestamp);

        let message = Message {
            time: self.last_timestamp,
            pred: self.last_message,
            parent,
            identity: client.session.identity,
            nick: client.session.nick.clone(),
            content,
        };

        self.last_message = message.id();
        info!(
            "&{} now at {} ({})",
            self.name, self.last_message, self.last_timestamp
        );

        self.notify_except(
            id,
            &Packet::ntf(SendNtf {
                message: message.clone(),
            }),
        );

        message
    }

    fn who(&self, id: SessionId) -> (Session, Vec<Session>) {
        let session = self.client(id).session.clone();
        let others = self
            .clients
            .values()
            .filter(|client| client.session.id != id)
            .map(|client| client.session.clone())
            .collect();
        (session, others)
    }
}

#[derive(Debug)]
struct ServerSession {
    tx: ConnTx,
    rx: ConnRx,
    room: Arc<Mutex<Room>>,
    session: Session,
}

impl ServerSession {
    async fn handle_nick(&mut self, id: u64, cmd: NickCmd) -> anyhow::Result<()> {
        if let Some(reason) = util::check_nick(&cmd.nick) {
            self.tx
                .send(&Packet::rpl(id, NickRpl::InvalidNick { reason }))?;
            return Ok(());
        }

        self.session.nick = cmd.nick.clone();
        self.tx.send(&Packet::rpl(
            id,
            NickRpl::Success {
                you: self.session.clone(),
            },
        ))?;
        self.room.lock().await.nick(self.session.id, cmd.nick);

        Ok(())
    }

    async fn handle_send(&mut self, id: u64, cmd: SendCmd) -> anyhow::Result<()> {
        if let Some(reason) = util::check_content(&cmd.content) {
            self.tx
                .send(&Packet::rpl(id, SendRpl::InvalidContent { reason }))?;
            return Ok(());
        }

        let message = self
            .room
            .lock()
            .await
            .send(self.session.id, cmd.parent, cmd.content);

        self.tx
            .send(&Packet::rpl(id, SendRpl::Success { message }))?;

        Ok(())
    }

    async fn handle_who(&mut self, id: u64, _cmd: WhoCmd) -> anyhow::Result<()> {
        let (you, others) = self.room.lock().await.who(self.session.id);
        self.tx.send(&Packet::rpl(id, WhoRpl { you, others }))?;
        Ok(())
    }

    async fn handle_packet(&mut self, packet: Packet) -> anyhow::Result<()> {
        match packet {
            Packet::Cmd { id, cmd } => match cmd {
                Cmd::Room(_) => Err(anyhow!("unexpected Room cmd")),
                Cmd::Identify(_) => Err(anyhow!("unexpected Identify cmd")),
                Cmd::Nick(cmd) => self.handle_nick(id, cmd).await,
                Cmd::Send(cmd) => self.handle_send(id, cmd).await,
                Cmd::Who(cmd) => self.handle_who(id, cmd).await,
            },
            Packet::Rpl { .. } => Err(anyhow!("unexpected rpl")),
            Packet::Ntf { .. } => Err(anyhow!("unexpected ntf")),
        }
    }

    async fn run(&mut self) -> anyhow::Result<()> {
        while let Some(packet) = self.rx.recv().await? {
            self.handle_packet(packet).await?;
        }
        Ok(())
    }
}

#[derive(Debug, Clone)]
struct Server {
    rooms: Arc<Mutex<HashMap<String, Arc<Mutex<Room>>>>>,
}

impl Server {
    fn new() -> Self {
        Self {
            rooms: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    async fn room(&self, name: String) -> Arc<Mutex<Room>> {
        self.rooms
            .lock()
            .await
            .entry(name.clone())
            .or_insert_with(|| Arc::new(Mutex::new(Room::new(name))))
            .clone()
    }

    async fn negotiate_room(tx: &ConnTx, rx: &mut ConnRx) -> anyhow::Result<String> {
        loop {
            match rx.recv().await? {
                Some(Packet::Cmd {
                    id,
                    cmd: Cmd::Room(RoomCmd { name }),
                }) => {
                    if let Some(reason) = util::check_room(&name) {
                        tx.send(&Packet::rpl(id, RoomRpl::InvalidRoom { reason }))?;
                        continue;
                    }
                    tx.send(&Packet::rpl(id, RoomRpl::Success))?;
                    return Ok(name);
                }
                Some(_) => bail!("invalid packet during room negotiation"),
                None => bail!("connection closed during room negotiation"),
            }
        }
    }

    async fn negotiate_identity(tx: &ConnTx, rx: &mut ConnRx) -> anyhow::Result<(u64, Session)> {
        loop {
            match rx.recv().await? {
                Some(Packet::Cmd {
                    id,
                    cmd: Cmd::Identify(IdentifyCmd { nick, identity }),
                }) => {
                    if let Some(reason) = util::check_identity(&identity) {
                        tx.send(&Packet::rpl(id, IdentifyRpl::InvalidNick { reason }))?;
                        continue;
                    }
                    if let Some(reason) = util::check_nick(&nick) {
                        tx.send(&Packet::rpl(id, IdentifyRpl::InvalidNick { reason }))?;
                        continue;
                    }
                    let session = Session {
                        id: SessionId::of(&format!("{}", rand::thread_rng().gen::<u64>())),
                        nick,
                        identity: Identity::of(&identity),
                    };
                    return Ok((id, session));
                }
                Some(_) => bail!("invalid packet during room negotiation"),
                None => bail!("connection closed during room negotiation"),
            }
        }
    }

    fn welcome(id: u64, you: Session, room: &Room, tx: &ConnTx) -> anyhow::Result<()> {
        let others = room
            .clients
            .values()
            .map(|client| client.session.clone())
            .collect::<Vec<_>>();
        let last_message = room.last_message;

        tx.send(&Packet::rpl(
            id,
            IdentifyRpl::Success {
                you,
                others,
                last_message,
            },
        ))?;

        Ok(())
    }

    async fn greet(&self, tx: ConnTx, mut rx: ConnRx) -> anyhow::Result<ServerSession> {
        let room = Self::negotiate_room(&tx, &mut rx).await?;
        let (id, session) = Self::negotiate_identity(&tx, &mut rx).await?;

        let room = self.room(room).await;
        {
            let mut room = room.lock().await;
            // Reply to successful identify command in the same lock as joining
            // the room so the client doesn' miss any messages.
            Self::welcome(id, session.clone(), &*room, &tx)?;
            // Join room only after welcome so current session is not yet
            // present in room during welcome.
            room.join(Client {
                session: session.clone(),
                send: tx.clone(),
            });
        }

        Ok(ServerSession {
            tx,
            rx,
            room,
            session,
        })
    }

    async fn greet_and_run(&self, tx: ConnTx, rx: ConnRx) -> anyhow::Result<()> {
        let mut session = self.greet(tx, rx).await?;
        let result = session.run().await;
        session.room.lock().await.part(session.session.id);
        result
    }

    /// Wrapper for [`ConnMaintenance::perform`] so it returns an
    /// [`anyhow::Result`].
    async fn maintain(maintenance: ConnMaintenance) -> anyhow::Result<()> {
        maintenance.perform().await?;
        Ok(())
    }

    async fn handle_conn(&self, stream: TcpStream) -> anyhow::Result<()> {
        let stream = MaybeTlsStream::Plain(stream);
        let stream = tokio_tungstenite::accept_async(stream).await?;
        let (tx, rx, maintenance) = conn::new(stream, Duration::from_secs(10));
        tokio::try_join!(self.greet_and_run(tx, rx), Self::maintain(maintenance))?;
        Ok(())
    }

    async fn on_conn(self, stream: TcpStream) -> anyhow::Result<()> {
        let peer_addr = stream.peer_addr()?;
        info!("<{peer_addr}> Connected");

        if let Err(e) = self.handle_conn(stream).await {
            warn!("<{peer_addr}> Err: {e}");
        }

        info!("<{peer_addr}> Disconnected");
        Ok(())
    }
}

#[tokio::main]
async fn main() {
    env_logger::init();

    let server = Server::new();
    let listener = TcpListener::bind(("::0", 40080)).await.unwrap();
    while let Ok((stream, _)) = listener.accept().await {
        tokio::spawn(server.clone().on_conn(stream));
    }
}

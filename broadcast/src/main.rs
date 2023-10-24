use std::collections::{HashMap, HashSet};

use anyhow::{Context, Result};
use maelstrom::{Client, ClientImpl, Message};
use serde::{Deserialize, Serialize};
use std::sync::{Arc, Mutex};
use tokio::{
    sync::mpsc::{channel, Sender},
    task::JoinHandle,
    time::Instant,
};

struct Broadcast {
    in_chan: Sender<Message<MessageIn>>,
    out_chan: Sender<Message<MessageOut>>,
}

#[derive(Serialize, Deserialize, Debug)]
#[serde(tag = "type")]
enum MessageIn {
    #[serde(rename = "broadcast")]
    Broadcast { message: u64 },
    #[serde(rename = "rebroadcast")]
    Rebroadcast {
        message: u64,
        visited: HashSet<String>,
    },
    #[serde(rename = "read")]
    Read {},
    #[serde(rename = "topology")]
    Topology {
        topology: HashMap<String, HashSet<String>>,
    },
}

#[derive(Serialize, Deserialize, Debug)]
#[serde(tag = "type")]
enum MessageOut {
    #[serde(rename = "broadcast_ok")]
    Broadcast {},
    #[serde(rename = "read_ok")]
    Read { messages: HashSet<u64> },
    #[serde(rename = "topology_ok")]
    Topology {},
}

impl ClientImpl<MessageIn, MessageOut> for Broadcast {
    fn on_msg(&mut self, msg: maelstrom::Message<MessageIn>, _client: &Client) -> Result<()> {
        self.in_chan
            .blocking_send(msg)
            .context("Failed to send message")?;

        Ok(())
    }

    fn on_reply(
        &mut self,
        msg: maelstrom::Message<MessageOut>,
        _client: &maelstrom::Client,
    ) -> Result<()> {
        self.out_chan
            .blocking_send(msg)
            .context("Failed to send message")?;
        Ok(())
    }
}

fn process_in_msg(
    msg: Message<MessageIn>,
    client: Arc<Client>,
    messages: Arc<Mutex<HashSet<u64>>>,
    topology: Arc<Mutex<Topology>>,
) -> Result<()> {
    match msg.data() {
        MessageIn::Broadcast { message } => {
            {
                let mut messages = messages
                    .lock()
                    .map_err(|_| anyhow::anyhow!("Failed to lock messages"))?;
                messages.insert(*message);
            }

            let guard = topology.lock().unwrap();
            let topology: &HashMap<String, Instant> = guard.as_ref().unwrap();
            let neighbors = topology.keys().collect::<HashSet<_>>();
            let mut visited = topology.keys().cloned().collect::<HashSet<_>>();
            visited.insert(client.node_id().to_owned());
            visited.extend(topology.keys().cloned());

            for node in neighbors {
                let msg = client.send_to(
                    node,
                    MessageIn::Rebroadcast {
                        message: *message,
                        visited: visited.clone(),
                    },
                );
                println!("{}", serde_json::to_value(msg)?);
            }

            println!(
                "{}",
                serde_json::to_value(client.reply(msg, MessageOut::Broadcast {}))?
            );
        }
        MessageIn::Rebroadcast { message, visited } => {
            {
                let mut messages = messages
                    .lock()
                    .map_err(|_| anyhow::anyhow!("Failed to lock messages"))?;
                messages.insert(*message);
            }

            let mut visited_nodes: HashSet<String> = visited.clone();

            let guard = topology.lock().unwrap();
            let topology: &HashMap<String, Instant> = guard.as_ref().unwrap();
            let neighbors = topology.keys().cloned().collect::<HashSet<_>>();

            visited_nodes.insert(client.node_id().to_owned());
            visited_nodes.extend(topology.keys().cloned());

            for node in neighbors.difference(visited) {
                let msg = client.send_to(
                    node,
                    MessageIn::Rebroadcast {
                        message: *message,
                        visited: visited_nodes.clone(),
                    },
                );
                println!("{}", serde_json::to_value(msg)?);
            }
        }
        MessageIn::Read {} => {
            println!(
                "{}",
                serde_json::to_value(client.reply(
                    msg,
                    MessageOut::Read {
                        messages: messages.lock().unwrap().clone(),
                    }
                ))?
            );
        }
        MessageIn::Topology {
            topology: in_topology,
        } => {
            let in_topology = in_topology.get(client.node_id()).ok_or(anyhow::anyhow!(
                "Failed to get topology for node {}",
                client.node_id()
            ))?;

            let node_topology: HashMap<String, Instant> = in_topology
                .iter()
                .cloned()
                .map(|n| (n, Instant::now()))
                .collect::<HashMap<_, _>>();

            *(topology.lock().unwrap()) = Some(node_topology);

            println!(
                "{}",
                serde_json::to_value(client.reply(msg, MessageOut::Topology {}))?
            );
        }
    }

    Ok(())
}

fn process_out_msg(
    msg: Message<MessageOut>,
    _client: Arc<Client>,
    messages: Arc<Mutex<HashSet<u64>>>,
    topology: Arc<Mutex<Topology>>,
) -> Result<()> {
    if let MessageOut::Read {
        messages: in_messages,
    } = msg.data()
    {
        {
            let mut guard = topology.lock().unwrap();
            let topology = guard.as_mut().unwrap();
            *topology.get_mut(msg.src()).unwrap() = Instant::now();
        }

        messages.lock().unwrap().extend(in_messages);
    }

    Ok(())
}

type Topology = Option<HashMap<String, Instant>>;

#[tokio::main]
async fn main() -> Result<()> {
    let mut client = Client::default();
    let (in_chan, mut in_rx) = channel(128);
    let (out_chan, mut out_rx) = channel(128);

    let topology = Arc::new(Mutex::new(None));
    let messages = Arc::new(Mutex::new(HashSet::new()));

    let broadcast = Broadcast { in_chan, out_chan };

    client.init().context("Failed to initialize client")?;

    let client = Arc::new(client);

    let in_msg: JoinHandle<Result<()>>;
    {
        let client = client.clone();
        let messages = messages.clone();
        let topology = topology.clone();

        in_msg = tokio::spawn(async move {
            while let Some(msg) = in_rx.recv().await {
                process_in_msg(msg, client.clone(), messages.clone(), topology.clone())?;
            }

            Ok(())
        });
    }

    let out_msg: JoinHandle<Result<()>>;
    {
        let client = client.clone();
        let messages = messages.clone();
        let topology: Arc<Mutex<Option<HashMap<String, Instant>>>> = topology.clone();

        out_msg = tokio::spawn(async move {
            while let Some(msg) = out_rx.recv().await {
                process_out_msg(msg, client.clone(), messages.clone(), topology.clone())?;
            }

            Ok(())
        });
    }

    let (quit_tx, mut quit_rx) = tokio::sync::oneshot::channel::<()>();

    let read_poll: JoinHandle<Result<()>>;
    {
        let client = client.clone();
        let topology: Arc<Mutex<Option<HashMap<String, Instant>>>> = topology.clone();

        read_poll = tokio::spawn(async move {
            use tokio::time::{timeout, Duration};

            while timeout(Duration::from_secs_f32(0.5), &mut quit_rx)
                .await
                .is_err()
            {
                let guard = topology.lock().unwrap();

                for (node, last_time) in guard.as_ref().unwrap() {
                    if last_time.elapsed() > Duration::from_secs_f32(1.0) {
                        let msg = client.send_to(node, MessageIn::Read {});
                        println!("{}", serde_json::to_value(msg)?);
                    }
                }
            }

            Ok(())
        })
    }

    let run_client: JoinHandle<Result<()>> = tokio::task::spawn_blocking(move || {
        client
            .run(broadcast)
            .context("Error while processing client requests")?;
        quit_tx
            .send(())
            .map_err(|_| anyhow::anyhow!("Failed to tear down read poll task"))?;

        Ok(())
    });

    let (i, o, p, c) = tokio::try_join!(in_msg, out_msg, read_poll, run_client)?;
    i?;
    o?;
    p?;
    c?;

    Ok(())
}

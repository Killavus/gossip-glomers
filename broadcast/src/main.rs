use std::collections::{HashMap, HashSet};

use anyhow::{Context, Result};
use maelstrom::{Client, ClientImpl, Message};
use serde::{Deserialize, Serialize};
use std::sync::{Arc, Mutex, OnceLock};
use tokio::{
    sync::mpsc::{channel, Sender},
    task::JoinHandle,
};

struct Broadcast {
    in_chan: Sender<Message<MessageIn>>,
    out_chan: Sender<Message<MessageOut>>,
}

#[derive(Serialize, Deserialize, Debug)]
#[serde(tag = "type")]
enum MessageIn {
    #[serde(rename = "broadcast")]
    Broadcast { message: serde_json::Value },
    #[serde(rename = "rebroadcast")]
    Rebroadcast {
        message: serde_json::Value,
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
    Read { messages: Vec<serde_json::Value> },
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
    messages: Arc<Mutex<Vec<serde_json::Value>>>,
    topology: Arc<OnceLock<HashMap<String, HashSet<String>>>>,
) -> Result<()> {
    match msg.data() {
        MessageIn::Broadcast { message } => {
            let mut messages = messages
                .lock()
                .map_err(|_| anyhow::anyhow!("Failed to lock messages"))?;
            messages.push(message.clone());

            let topology = topology
                .get()
                .unwrap()
                .get(client.node_id())
                .ok_or(anyhow::anyhow!(
                    "Failed to get topology for node {}",
                    client.node_id()
                ))?;

            let mut visited = topology.clone();
            visited.insert(client.node_id().to_owned());

            for node in topology {
                let msg = client.send_to(
                    node,
                    MessageIn::Rebroadcast {
                        message: message.clone(),
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
            let mut messages = messages
                .lock()
                .map_err(|_| anyhow::anyhow!("Failed to lock messages"))?;
            messages.push(message.clone());
            let mut visited_nodes: HashSet<String> = visited.clone();

            let topology = topology
                .get()
                .unwrap()
                .get(client.node_id())
                .ok_or(anyhow::anyhow!(
                    "Failed to get topology for node {}",
                    client.node_id()
                ))?;

            visited_nodes.insert(client.node_id().to_owned());
            visited_nodes.extend(topology.iter().map(|n| n.to_owned()));

            for node in topology.difference(visited) {
                let msg = client.send_to(
                    node,
                    MessageIn::Rebroadcast {
                        message: message.clone(),
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
            topology.set(in_topology.clone()).ok();

            println!(
                "{}",
                serde_json::to_value(client.reply(msg, MessageOut::Topology {}))?
            );
        }
    }

    Ok(())
}

fn process_out_msg(
    _msg: Message<MessageOut>,
    _client: Arc<Client>,
    _messages: Arc<Mutex<Vec<serde_json::Value>>>,
    _topology: Arc<OnceLock<HashMap<String, HashSet<String>>>>,
) -> Result<()> {
    Ok(())
}

#[tokio::main]
async fn main() -> Result<()> {
    let mut client = Client::default();
    let (in_chan, mut in_rx) = channel(128);
    let (out_chan, mut out_rx) = channel(128);

    let topology = Arc::new(OnceLock::<HashMap<String, HashSet<String>>>::new());
    let messages = Arc::new(Mutex::new(Vec::<serde_json::Value>::new()));

    let broadcast = Broadcast { in_chan, out_chan };

    client.init().context("Failed to initialize client")?;

    let client = Arc::new(client);

    let client_ = client.clone();
    let messages_ = messages.clone();
    let topology_ = topology.clone();

    let in_msg: JoinHandle<Result<()>> = tokio::spawn(async move {
        while let Some(msg) = in_rx.recv().await {
            process_in_msg(msg, client_.clone(), messages_.clone(), topology_.clone())?;
        }

        Ok(())
    });

    let client_ = client.clone();
    let messages_ = messages.clone();
    let topology_ = topology.clone();

    let out_msg: JoinHandle<Result<()>> = tokio::spawn(async move {
        while let Some(msg) = out_rx.recv().await {
            process_out_msg(msg, client_.clone(), messages_.clone(), topology_.clone())?;
        }

        Ok(())
    });

    let run_client: JoinHandle<Result<()>> = tokio::task::spawn_blocking(move || {
        client
            .run(broadcast)
            .context("Error while processing client requests")?;
        Ok(())
    });

    let (i, o, c) = tokio::try_join!(in_msg, out_msg, run_client)?;
    i?;
    o?;
    c?;

    Ok(())
}

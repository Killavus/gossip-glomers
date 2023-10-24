use std::collections::{HashMap, HashSet};

use anyhow::{Context, Result};
use maelstrom::{Client, ClientImpl};
use serde::{Deserialize, Serialize};

#[derive(Default)]
struct Broadcast {
    topology: HashMap<String, HashSet<String>>,
    messages: Vec<serde_json::Value>,
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
    fn on_msg(&mut self, msg: maelstrom::Message<MessageIn>, client: &Client) -> Result<()> {
        match msg.data() {
            MessageIn::Broadcast { message } => {
                self.messages.push(message.clone());

                let topology = self.topology.get(client.node_id()).ok_or(anyhow::anyhow!(
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
                self.messages.push(message.clone());
                let mut visited_nodes: HashSet<String> = visited.clone();

                let topology = self.topology.get(client.node_id()).ok_or(anyhow::anyhow!(
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
                            messages: self.messages.clone(),
                        }
                    ))?
                );
            }
            MessageIn::Topology { topology } => {
                self.topology = topology.clone();
                println!(
                    "{}",
                    serde_json::to_value(client.reply(msg, MessageOut::Topology {}))?
                );
            }
        }

        Ok(())
    }

    fn on_reply(
        &mut self,
        _msg: maelstrom::Message<MessageOut>,
        _client: &maelstrom::Client,
    ) -> Result<()> {
        Ok(())
    }
}

fn main() -> Result<()> {
    let mut client = Client::default();
    client.init().context("Failed to initialize client")?;
    client
        .run(Broadcast::default())
        .context("Error while processing client requests")?;

    Ok(())
}

use std::collections::HashMap;

use anyhow::{Context, Result};
use maelstrom::{Client, ClientImpl};
use serde::{Deserialize, Serialize};

#[derive(Default)]
struct Broadcast {
    topology: HashMap<String, Vec<String>>,
    messages: Vec<serde_json::Value>,
}

#[derive(Serialize, Deserialize, Debug)]
#[serde(tag = "type")]
enum MessageIn {
    #[serde(rename = "broadcast")]
    Broadcast { message: serde_json::Value },
    #[serde(rename = "read")]
    Read {},
    #[serde(rename = "topology")]
    Topology {
        topology: HashMap<String, Vec<String>>,
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

impl ClientImpl<MessageIn> for Broadcast {
    fn on_msg(&mut self, msg: maelstrom::Message<MessageIn>, client: &Client) -> Result<()> {
        match msg.data() {
            MessageIn::Broadcast { message } => {
                self.messages.push(message.clone());
                println!(
                    "{}",
                    serde_json::to_value(client.reply(msg, MessageOut::Broadcast {}))?
                )
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
}

fn main() -> Result<()> {
    let mut client = Client::default();
    client.init().context("Failed to initialize client")?;
    client
        .run(Broadcast::default())
        .context("Error while processing client requests")?;

    Ok(())
}

use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};

mod message;

use message::{InMessage, UninitMessageIn, UninitMessageOut};

#[derive(Debug, Default)]
enum EchoServer {
    #[default]
    NotStarted,
    Started {
        id: String,
        neighbours: Vec<String>,
    },
}

#[derive(Serialize, Deserialize, Debug)]
#[serde(tag = "type")]
enum EchoBodyIn {
    #[serde(rename = "echo")]
    Echo { echo: String },
}

#[derive(Serialize, Deserialize, Debug)]
#[serde(tag = "type")]
enum EchoBodyOut {
    #[serde(rename = "echo_ok")]
    EchoOk { echo: String },
}

impl EchoServer {
    fn run(&mut self) -> Result<()> {
        use std::io;
        use EchoServer::*;

        for line in io::stdin().lines() {
            let line = line.context("failed to read line from stdin")?;

            match self {
                NotStarted => {
                    let msg: InMessage<UninitMessageIn> =
                        serde_json::from_str(&line).context("Failed to parse line - got {line}")?;
                    let UninitMessageIn::Init { node_id, node_ids } = msg.data();

                    *self = Self::Started {
                        id: node_id.clone(),
                        neighbours: node_ids.clone(),
                    };

                    println!(
                        "{}",
                        serde_json::to_value(msg.into_reply(None, UninitMessageOut::InitOk {}))?
                    );
                }
                Started { .. } => {
                    let msg: InMessage<EchoBodyIn> =
                        serde_json::from_str(&line).context("Failed to parse line - got {line}")?;

                    println!(
                        "{}",
                        serde_json::to_value(msg.into_reply_with(None, |data| {
                            let EchoBodyIn::Echo { echo } = data;
                            EchoBodyOut::EchoOk { echo }
                        }))?
                    );
                }
            }
        }

        Ok(())
    }
}

fn main() -> Result<()> {
    let mut server = EchoServer::default();

    server.run()?;

    Ok(())
}

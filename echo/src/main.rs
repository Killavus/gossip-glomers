use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};

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
struct Message {
    src: String,
    #[serde(rename = "dest")]
    dst: String,
    body: MessageBody,
}

#[derive(Serialize, Deserialize, Debug)]
#[serde(tag = "type")]
enum MessageBody {
    #[serde(rename = "init")]
    Init {
        msg_id: Option<serde_json::Value>,
        node_id: String,
        node_ids: Vec<String>,
    },
    #[serde(rename = "init_ok")]
    InitOk {
        in_reply_to: Option<serde_json::Value>,
    },
    #[serde(rename = "echo")]
    Echo {
        msg_id: Option<serde_json::Value>,
        echo: String,
    },
    #[serde(rename = "echo_ok")]
    EchoOk {
        in_reply_to: Option<serde_json::Value>,
        echo: String,
    },
}

impl EchoServer {
    fn run(&mut self) -> Result<()> {
        use std::io;
        use EchoServer::*;

        for line in io::stdin().lines() {
            let line = line.context("failed to read line from stdin")?;
            let msg: Message =
                serde_json::from_str(&line).context("Failed to parse line - got {line}")?;

            match self {
                NotStarted => {
                    if let MessageBody::Init {
                        msg_id,
                        node_id,
                        node_ids,
                    } = msg.body
                    {
                        let reply = Message {
                            src: node_id.clone(),
                            dst: msg.src.clone(),
                            body: MessageBody::InitOk {
                                in_reply_to: msg_id,
                            },
                        };

                        *self = Started {
                            id: node_id,
                            neighbours: node_ids,
                        };

                        println!("{}", serde_json::to_string(&reply)?);
                    }
                }
                Started { .. } => {
                    if let MessageBody::Echo { msg_id, echo } = msg.body {
                        let reply = Message {
                            src: msg.dst.clone(),
                            dst: msg.src.clone(),
                            body: MessageBody::EchoOk {
                                in_reply_to: msg_id,
                                echo: echo.clone(),
                            },
                        };

                        println!("{}", serde_json::to_value(&reply)?);
                    }
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

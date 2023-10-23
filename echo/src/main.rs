use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};

mod message;

use message::{InMessage, UninitMessageIn, UninitMessageOut};

#[derive(Debug, Default)]
struct Echo {
    next_id: u64,
    state: ServerState,
}

#[derive(Debug, Default)]
enum ServerState {
    #[default]
    NotStarted,
    Started,
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

impl Echo {
    fn run(&mut self) -> Result<()> {
        use std::io;
        use ServerState::*;

        for line in io::stdin().lines() {
            let line = line.context("failed to read line from stdin")?;

            match self.state {
                NotStarted => {
                    let msg: InMessage<UninitMessageIn> =
                        serde_json::from_str(&line).context("Failed to parse line - got {line}")?;
                    let UninitMessageIn::Init { .. } = msg.data();

                    self.state = ServerState::Started;

                    println!(
                        "{}",
                        serde_json::to_value(
                            msg.into_reply(self.next_id(), UninitMessageOut::InitOk {})
                        )?
                    );
                }
                Started { .. } => {
                    let msg: InMessage<EchoBodyIn> =
                        serde_json::from_str(&line).context("Failed to parse line - got {line}")?;

                    println!(
                        "{}",
                        serde_json::to_value(msg.into_reply_with(self.next_id(), |data| {
                            let EchoBodyIn::Echo { echo } = data;
                            EchoBodyOut::EchoOk { echo }
                        }))?
                    );
                }
            }
        }

        Ok(())
    }

    fn next_id(&mut self) -> Option<u64> {
        self.next_id += 1;
        Some(self.next_id)
    }
}

fn main() -> Result<()> {
    let mut server = Echo::default();

    server.run()?;

    Ok(())
}

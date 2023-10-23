use anyhow::{Context, Result};
use client::Client;
use serde::{Deserialize, Serialize};

mod client;
mod message;

use message::Message;

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

#[derive(Default)]
struct Echo {}

impl client::ClientImpl<EchoBodyIn> for Echo {
    fn on_msg(&mut self, msg: Message<EchoBodyIn>, client: &Client) -> Result<()> {
        println!(
            "{}",
            serde_json::to_value(client.reply_with(msg, |data| {
                let EchoBodyIn::Echo { echo } = data;
                EchoBodyOut::EchoOk { echo }
            }))?
        );

        Ok(())
    }
}

fn main() -> Result<()> {
    let mut client = Client::default();
    client.init().context("Error while initializing client")?;
    client.run(Echo {}).context("Error while running client")?;

    Ok(())
}

use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};

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

impl maelstrom::ClientImpl<EchoBodyIn, EchoBodyOut> for Echo {
    fn on_msg(
        &mut self,
        msg: maelstrom::Message<EchoBodyIn>,
        client: &maelstrom::Client,
    ) -> Result<()> {
        println!(
            "{}",
            serde_json::to_value(client.reply_with(msg, |data| {
                let EchoBodyIn::Echo { echo } = data;
                EchoBodyOut::EchoOk { echo }
            }))?
        );

        Ok(())
    }

    fn on_reply(
        &mut self,
        _msg: maelstrom::Message<EchoBodyOut>,
        _client: &maelstrom::Client,
    ) -> Result<()> {
        Ok(())
    }
}

fn main() -> Result<()> {
    let mut client = maelstrom::Client::default();
    client.init().context("Error while initializing client")?;
    client.run(Echo {}).context("Error while running client")?;

    Ok(())
}

use anyhow::{Context, Result};
use maelstrom::{Client, ClientImpl};
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, Debug)]
#[serde(tag = "type")]
enum MessageIn {
    #[serde(rename = "generate")]
    Generate {},
}

#[derive(Serialize, Deserialize, Debug)]
#[serde(tag = "type")]
enum MessageOut {
    #[serde(rename = "generate_ok")]
    GenerateOk { id: String },
}

#[derive(Default)]
struct UniqueId {
    id: u64,
}

impl ClientImpl<MessageIn, MessageOut> for UniqueId {
    fn on_msg(&mut self, msg: maelstrom::Message<MessageIn>, client: &Client) -> Result<()> {
        println!(
            "{}",
            serde_json::to_value(client.reply_with(msg, |data| {
                let MessageIn::Generate {} = data;
                self.id += 1;
                MessageOut::GenerateOk {
                    id: format!("{}.{}", client.node_id(), self.id),
                }
            }))?
        );

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
    client.init().context("Error while initializing client")?;
    client
        .run(UniqueId::default())
        .context("Error while running client")?;

    Ok(())
}

use std::sync::atomic::AtomicU64;

use crate::message::Message;
use anyhow::{Context, Result};
use serde::{de::DeserializeOwned, Deserialize, Serialize};

#[derive(Serialize, Deserialize, Clone)]
#[serde(tag = "type")]
pub enum UninitMessageIn {
    #[serde(rename = "init")]
    Init {
        node_id: String,
        node_ids: Vec<String>,
    },
}

#[derive(Serialize, Deserialize, Clone)]
#[serde(tag = "type")]
pub enum UninitMessageOut {
    #[serde(rename = "init_ok")]
    InitOk {},
}

#[derive(Debug, Default)]
pub enum ClientState {
    #[default]
    Uninit,
    Init {
        node_id: String,
        node_ids: Vec<String>,
    },
}

pub trait ClientImpl<In> {
    fn on_msg(&mut self, msg: Message<In>, client: &Client) -> Result<()>;
}

#[derive(Default)]
pub struct Client {
    next_id: AtomicU64,
    pub state: ClientState,
}

impl Client {
    pub fn init(&mut self) -> Result<()> {
        if matches!(self.state, ClientState::Init { .. }) {
            anyhow::bail!("Client already initialized");
        }

        use std::io;
        let mut line = String::new();
        io::stdin()
            .read_line(&mut line)
            .context("failed to read first line")?;

        let line = line.trim();

        let msg: Message<UninitMessageIn> =
            serde_json::from_str(&line).context("Failed to parse init message - got {line}")?;

        let reply = msg.into_reply_with(
            self.next_id(),
            |UninitMessageIn::Init { node_id, node_ids }| {
                self.state = ClientState::Init { node_id, node_ids };
                UninitMessageOut::InitOk {}
            },
        );

        println!("{}", serde_json::to_value(reply)?);
        Ok(())
    }

    pub fn reply<In, Out>(&self, msg: Message<In>, data: Out) -> Message<Out> {
        msg.into_reply(self.next_id(), data)
    }

    pub fn reply_with<In, Out, F: FnOnce(In) -> Out>(
        &self,
        msg: Message<In>,
        data_fn: F,
    ) -> Message<Out> {
        msg.into_reply_with(self.next_id(), data_fn)
    }

    pub fn next_id(&self) -> Option<u64> {
        use std::sync::atomic::Ordering;

        Some(self.next_id.fetch_add(1, Ordering::AcqRel))
    }

    pub fn run<In: DeserializeOwned>(&self, mut client_impl: impl ClientImpl<In>) -> Result<()> {
        use std::io;

        if let ClientState::Init { .. } = &self.state {
            for line in io::stdin().lines() {
                let line = line.context("failed to read line")?;
                let msg: Message<In> =
                    serde_json::from_str(&line).context("Failed to parse message - got {line}")?;

                client_impl.on_msg(msg, self)?;
            }
        } else {
            anyhow::bail!("Client not initialized - run init first");
        }

        Ok(())
    }
}

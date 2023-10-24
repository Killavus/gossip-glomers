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

pub trait ClientImpl<In, Out> {
    fn on_msg(&mut self, msg: Message<In>, client: &Client) -> Result<()>;
    fn on_reply(&mut self, msg: Message<Out>, client: &Client) -> Result<()>;
}

#[derive(Default)]
pub struct Client {
    next_id: AtomicU64,
    pub state: ClientState,
}

impl Client {
    pub fn node_id(&self) -> &str {
        match &self.state {
            ClientState::Init { node_id, .. } => node_id,
            _ => panic!("Client not initialized"),
        }
    }

    pub fn node_ids(&self) -> &Vec<String> {
        match &self.state {
            ClientState::Init { node_ids, .. } => node_ids,
            _ => panic!("Client not initialized"),
        }
    }

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
            serde_json::from_str(line).context("Failed to parse init message - got {line}")?;

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

    pub fn send_to<D>(&self, dst: &str, data: D) -> Message<D> {
        Message::new(
            self.node_id().to_owned(),
            dst.to_owned(),
            self.next_id(),
            data,
        )
    }

    pub fn run<In: DeserializeOwned, Out: DeserializeOwned>(
        &self,
        mut client_impl: impl ClientImpl<In, Out>,
    ) -> Result<()> {
        use std::io;

        if let ClientState::Init { .. } = &self.state {
            for line in io::stdin().lines() {
                let line = line.context("failed to read line")?;
                let in_msg = serde_json::from_str(&line);

                if let Ok(msg) = in_msg {
                    client_impl.on_msg(msg, self)?;
                } else {
                    let out_msg: Message<Out> = serde_json::from_str(&line)?;
                    client_impl.on_reply(out_msg, self)?;
                }
            }
        } else {
            anyhow::bail!("Client not initialized - run init first");
        }

        Ok(())
    }
}

use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, Debug)]
pub struct Message<In> {
    src: String,
    #[serde(rename = "dest")]
    dst: String,
    body: MessageBody<In>,
}

#[derive(Serialize, Deserialize, Debug)]
struct MessageBody<D> {
    msg_id: Option<u64>,
    in_reply_to: Option<u64>,
    #[serde(flatten)]
    data: D,
}

impl<In> Message<In> {
    pub fn into_reply<Out>(self, msg_id: Option<u64>, data: Out) -> Message<Out> {
        Message {
            src: self.dst,
            dst: self.src,
            body: MessageBody {
                msg_id,
                in_reply_to: self.body.msg_id,
                data,
            },
        }
    }

    pub fn into_reply_with<Out, F: FnOnce(In) -> Out>(
        self,
        msg_id: Option<u64>,
        data_fn: F,
    ) -> Message<Out> {
        Message {
            src: self.dst,
            dst: self.src,
            body: MessageBody {
                msg_id,
                in_reply_to: self.body.msg_id,
                data: data_fn(self.body.data),
            },
        }
    }

    pub fn data(&self) -> &In {
        &self.body.data
    }
}

#[derive(Serialize, Deserialize, Clone)]
#[serde(tag = "type")]
pub enum UninitMessageIn {
    #[serde(rename = "init")]
    Init {
        node_id: String,
        node_ids: Vec<String>,
    },
}

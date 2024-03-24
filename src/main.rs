use serde::{Deserialize, Serialize};
use std::io::{self};

#[derive(Serialize, Deserialize)]
struct Message<B> {
    src: String,
    dest: String,
    body: B,
}

#[derive(Serialize, Deserialize)]
struct GenericBody {
    #[serde(rename = "type")]
    msg_type: String,
    msg_id: Option<usize>,
}

#[derive(Serialize, Deserialize)]
struct EchoBody {
    #[serde(rename = "type")]
    msg_type: String,
    msg_id: Option<usize>,
    in_reply_to: Option<usize>,
    echo: String,
}

#[derive(Serialize, Deserialize)]
struct InitBody {
    #[serde(rename = "type")]
    msg_type: String,
    in_reply_to: Option<usize>,
    #[serde(skip_serializing_if = "Option::is_none")]
    msg_id: Option<usize>,
    #[serde(skip_serializing_if = "Option::is_none")]
    node_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    node_ids: Option<Vec<String>>,
}

#[derive(Serialize, Deserialize)]
struct GenerateBody {
    #[serde(rename = "type")]
    msg_type: String,
    id: Option<String>,
    msg_id: Option<usize>,
    in_reply_to: Option<usize>,
}

#[derive(Serialize, Deserialize)]
struct BroadcastBody {
    #[serde(rename = "type")]
    msg_type: String,
    msg_id: Option<usize>,
    in_reply_to: Option<usize>,
    #[serde(skip_serializing_if = "Option::is_none")]
    message: Option<usize>,
}

#[derive(Serialize, Deserialize)]
struct ReadBody {
    #[serde(rename = "type")]
    msg_type: String,
    msg_id: Option<usize>,
    in_reply_to: Option<usize>,
    messages: Option<Vec<usize>>,
}

#[derive(Serialize, Deserialize)]
struct TopologyBody {
    #[serde(rename = "type")]
    msg_type: String,
    msg_id: Option<usize>,
    in_reply_to: Option<usize>,
    // topology: Option<Topology>, but we don't care for now
}

#[derive(Serialize, Deserialize)]
struct ErrorBody {
    #[serde(rename = "type")]
    msg_type: String,
    code: usize,
    #[serde(skip_serializing_if = "Option::is_none")]
    in_reply_to: Option<usize>,
}

fn main() {
    let mut node_id = String::new();
    let mut id_idx = 0;
    let mut msg_idx = 1;
    let mut broadcast_messages: Vec<usize> = Vec::new();

    loop {
        let mut input = String::new();
        io::stdin().read_line(&mut input).unwrap();

        eprintln!("input: {}", input);

        let input_msg: Message<GenericBody> = serde_json::from_str(&input).unwrap();

        match input_msg.body.msg_type.as_str() {
            "echo" => {
                let input_msg: Message<EchoBody> = serde_json::from_str(&input).unwrap();

                let output_msg = Message {
                    src: input_msg.dest,
                    dest: input_msg.src,
                    body: EchoBody {
                        msg_type: "echo_ok".to_string(),
                        msg_id: Some(msg_idx),
                        in_reply_to: input_msg.body.msg_id,
                        echo: input_msg.body.echo,
                    },
                };

                let output = serde_json::to_string(&output_msg).unwrap();
                println!("{}", output);
                eprintln!("output: {}", output);
            }
            "init" => {
                let input_msg: Message<InitBody> = serde_json::from_str(&input).unwrap();

                node_id = input_msg.body.node_id.unwrap();

                let output_msg = Message {
                    src: input_msg.dest,
                    dest: input_msg.src,
                    body: InitBody {
                        msg_type: "init_ok".to_string(),
                        in_reply_to: input_msg.body.msg_id,

                        node_id: None,
                        node_ids: None,
                        msg_id: None,
                    },
                };

                let output = serde_json::to_string(&output_msg).unwrap();
                println!("{}", output);
                eprintln!("output: {}", output);
            }
            "generate" => {
                let input_msg: Message<GenerateBody> = serde_json::from_str(&input).unwrap();

                let id = format!("{}-{}", node_id, id_idx);
                id_idx += 1;

                let output_msg = Message {
                    src: input_msg.dest,
                    dest: input_msg.src,
                    body: GenerateBody {
                        msg_type: "generate_ok".to_string(),
                        id: Some(id),
                        msg_id: Some(msg_idx),
                        in_reply_to: input_msg.body.msg_id,
                    },
                };

                let output = serde_json::to_string(&output_msg).unwrap();
                println!("{}", output);
                eprintln!("output: {}", output);
            }
            "broadcast" => {
                let input_msg: Message<BroadcastBody> = serde_json::from_str(&input).unwrap();

                broadcast_messages.push(input_msg.body.message.unwrap());

                let output_msg = Message {
                    src: input_msg.dest,
                    dest: input_msg.src,
                    body: BroadcastBody {
                        msg_type: "broadcast_ok".to_string(),
                        msg_id: Some(msg_idx),
                        in_reply_to: input_msg.body.msg_id,
                        message: None,
                    },
                };

                let output = serde_json::to_string(&output_msg).unwrap();
                println!("{}", output);
                eprintln!("output: {}", output);
            }
            "read" => {
                let input_msg: Message<ReadBody> = serde_json::from_str(&input).unwrap();

                let output_msg = Message {
                    src: input_msg.dest,
                    dest: input_msg.src,
                    body: ReadBody {
                        msg_type: "read_ok".to_string(),
                        msg_id: Some(msg_idx),
                        in_reply_to: input_msg.body.msg_id,
                        messages: Some(broadcast_messages.clone()),
                    },
                };

                let output = serde_json::to_string(&output_msg).unwrap();
                println!("{}", output);
                eprintln!("output: {}", output);
            }
            "topology" => {
                let input_msg: Message<TopologyBody> = serde_json::from_str(&input).unwrap();

                let output_msg = Message {
                    src: input_msg.dest,
                    dest: input_msg.src,
                    body: TopologyBody {
                        msg_type: "topology_ok".to_string(),
                        in_reply_to: input_msg.body.msg_id,
                        msg_id: Some(msg_idx),
                    },
                };

                let output = serde_json::to_string(&output_msg).unwrap();
                println!("{}", output);
                eprintln!("output: {}", output);
            }

            _ => {
                let output_msg = Message {
                    src: input_msg.dest,
                    dest: input_msg.src,
                    body: ErrorBody {
                        msg_type: "error".to_string(),
                        code: 12,
                        in_reply_to: input_msg.body.msg_id,
                    },
                };

                let output = serde_json::to_string(&output_msg).unwrap();
                println!("{}", output);
                eprintln!("output: {}", output);
            }
        }
        msg_idx += 1;
    }
}

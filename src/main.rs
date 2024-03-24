use serde::{Deserialize, Serialize};
use std::io;

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
    msg_id: Option<usize>,
    in_reply_to: Option<usize>,
    node_id: Option<String>,
    node_ids: Option<Vec<String>>,
}

fn main() {
    loop {
        let mut input = String::new();
        io::stdin().read_line(&mut input).unwrap();

        let input_msg: Message<GenericBody> = serde_json::from_str(&input).unwrap();

        match input_msg.body.msg_type.as_str() {
            "echo" => {
                let input_msg: Message<EchoBody> = serde_json::from_str(&input).unwrap();

                let output_msg = Message {
                    src: input_msg.dest,
                    dest: input_msg.src,
                    body: EchoBody {
                        msg_type: "echo_ok".to_string(),
                        msg_id: input_msg.body.msg_id,
                        in_reply_to: input_msg.body.msg_id,
                        echo: input_msg.body.echo,
                    },
                };

                let output = serde_json::to_string(&output_msg).unwrap();
                println!("{}", output);
            }
            "init" => {
                let input_msg: Message<InitBody> = serde_json::from_str(&input).unwrap();

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
            }
            _ => {
                eprint!("unknown message type: {}\n", input_msg.body.msg_type);
            }
        }
    }
}

use serde::{Deserialize, Serialize};
use std::{
    collections::HashMap,
    io,
    sync::{atomic::AtomicUsize, Arc, Mutex},
};
use tokio::task;

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
    #[serde(skip_serializing_if = "Option::is_none")]
    topology: Option<HashMap<String, Vec<String>>>,
}
#[derive(Serialize, Deserialize)]
struct ErrorBody {
    #[serde(rename = "type")]
    msg_type: String,
    code: usize,
    #[serde(skip_serializing_if = "Option::is_none")]
    in_reply_to: Option<usize>,
}

fn atomic_increment(counter: &Arc<AtomicUsize>) -> usize {
    counter.fetch_add(1, std::sync::atomic::Ordering::SeqCst)
}

async fn stdout_handler(stdout_chan_receiver: &flume::Receiver<String>) {
    for msg in stdout_chan_receiver.iter() {
        println!("{}", msg);
    }
}

async fn init_handler(
    in_init_chan_receiver: flume::Receiver<Message<InitBody>>,
    node_id_for_generate_chan_sender: flume::Sender<String>,
    node_id_for_network_chan_sender: flume::Sender<String>,
    stdout_chan_sender: flume::Sender<String>,
    atomic_counter: Arc<AtomicUsize>,
) {
    for input_msg in in_init_chan_receiver.iter() {
        let node_id = input_msg.body.node_id.unwrap();

        let output_msg = Message {
            src: input_msg.dest,
            dest: input_msg.src,
            body: InitBody {
                msg_type: "init_ok".to_string(),
                in_reply_to: input_msg.body.msg_id,
                msg_id: Some(atomic_increment(&atomic_counter)),
                node_id: None,
                node_ids: None,
            },
        };

        node_id_for_generate_chan_sender
            .send(node_id.clone())
            .unwrap();
        node_id_for_network_chan_sender
            .send(node_id.clone())
            .unwrap();

        let output = serde_json::to_string(&output_msg).unwrap();
        stdout_chan_sender.send(output).unwrap();
    }
}

async fn echo_handler(
    in_echo_chan_receiver: flume::Receiver<Message<EchoBody>>,
    stdout_chan_sender: flume::Sender<String>,
    atomic_counter: Arc<AtomicUsize>,
) {
    for input_msg in in_echo_chan_receiver.iter() {
        let output_msg = Message {
            src: input_msg.dest,
            dest: input_msg.src,
            body: EchoBody {
                msg_type: "echo_ok".to_string(),
                msg_id: Some(atomic_increment(&atomic_counter)),
                in_reply_to: input_msg.body.msg_id,
                echo: input_msg.body.echo,
            },
        };

        let output = serde_json::to_string(&output_msg).unwrap();
        stdout_chan_sender.send(output).unwrap();
    }
}

async fn generate_handler(
    in_generate_chan_receiver: flume::Receiver<Message<GenerateBody>>,
    node_id_chan_receiver: flume::Receiver<String>,
    stdout_chan_sender: flume::Sender<String>,
    atomic_counter: Arc<AtomicUsize>,
) {
    let mut node_id = String::new();

    // Wait for node_id
    for msg in node_id_chan_receiver.iter() {
        node_id = msg;
        break;
    }

    let mut id_idx = 0;

    for input_msg in in_generate_chan_receiver.iter() {
        // Generate a unique id: unique on the node because increment, and unique across the network because node_id
        let id = format!("{}-{}", node_id, id_idx);
        id_idx += 1;

        let output_msg = Message {
            src: input_msg.dest,
            dest: input_msg.src,
            body: GenerateBody {
                msg_type: "generate_ok".to_string(),
                id: Some(id),
                msg_id: Some(atomic_increment(&atomic_counter)),
                in_reply_to: input_msg.body.msg_id,
            },
        };

        let output = serde_json::to_string(&output_msg).unwrap();
        stdout_chan_sender.send(output).unwrap();
    }
}

// network_handler is reponsible for handling topology messages
// as well as broadcast/read messages and gossiping them to neighbors
async fn network_handler(
    node_id_chan_receiver: flume::Receiver<String>,
    in_topology_chan_receiver: flume::Receiver<Message<TopologyBody>>,
    in_broadcast_chan_receiver: flume::Receiver<Message<BroadcastBody>>,
    in_read_chan_receiver: flume::Receiver<Message<ReadBody>>,
    stdout_chan_sender: flume::Sender<String>,
    atomic_counter: Arc<AtomicUsize>,
) {
    let mut node_id = String::new();
    let neighbors: Mutex<Vec<String>> = Mutex::new(Vec::new());
    let broadcast_messages: Mutex<Vec<usize>> = Mutex::new(Vec::new());

    // Wait for node_id
    for msg in node_id_chan_receiver.iter() {
        node_id = msg;
        break;
    }

    loop {
        flume::Selector::new()
            .recv(&in_topology_chan_receiver, |input_msg| {
                let input_msg = input_msg.unwrap();

                let output_msg = Message {
                    src: input_msg.dest.clone(),
                    dest: input_msg.src.clone(),
                    body: TopologyBody {
                        msg_type: "topology_ok".to_string(),
                        in_reply_to: input_msg.body.msg_id,
                        msg_id: Some(atomic_increment(&atomic_counter)),
                        topology: None,
                    },
                };

                let output = serde_json::to_string(&output_msg).unwrap();
                stdout_chan_sender.send(output).unwrap();

                neighbors.lock().unwrap().clear();
                neighbors.lock().unwrap().extend(
                    input_msg
                        .body
                        .topology
                        .unwrap()
                        .get(&node_id)
                        .unwrap()
                        .clone(),
                );
            })
            .recv(&in_broadcast_chan_receiver, |input_msg| {
                let input_msg = input_msg.unwrap();

                let output_msg = Message {
                    src: input_msg.dest,
                    dest: input_msg.src,
                    body: BroadcastBody {
                        msg_type: "broadcast_ok".to_string(),
                        msg_id: Some(atomic_increment(&atomic_counter)),
                        in_reply_to: input_msg.body.msg_id,
                        message: None,
                    },
                };

                let output = serde_json::to_string(&output_msg).unwrap();
                stdout_chan_sender.send(output).unwrap();

                // If we've already seen this message, don't broadcast it again
                if broadcast_messages
                    .lock()
                    .unwrap()
                    .contains(&input_msg.body.message.unwrap())
                {
                    return;
                }

                broadcast_messages
                    .lock()
                    .unwrap()
                    .push(input_msg.body.message.unwrap());

                // broadcast to neighbors
                for neighbor in neighbors.lock().unwrap().iter() {
                    let output_msg = Message {
                        src: node_id.clone(),
                        dest: neighbor.clone(),
                        body: BroadcastBody {
                            msg_type: "broadcast".to_string(),
                            msg_id: None, // Inter-server messages don't have a msg_id, and don't need a response
                            in_reply_to: None,
                            message: Some(input_msg.body.message.unwrap()),
                        },
                    };

                    let output = serde_json::to_string(&output_msg).unwrap();
                    stdout_chan_sender.send(output).unwrap();
                }
            })
            .recv(&in_read_chan_receiver, |input_msg| {
                let input_msg = input_msg.unwrap();
                let messages = broadcast_messages.lock().unwrap().clone();

                let output_msg = Message {
                    src: input_msg.dest,
                    dest: input_msg.src,
                    body: ReadBody {
                        msg_type: "read_ok".to_string(),
                        msg_id: Some(atomic_increment(&atomic_counter)),
                        in_reply_to: input_msg.body.msg_id,
                        messages: Some(messages),
                    },
                };

                let output = serde_json::to_string(&output_msg).unwrap();
                stdout_chan_sender.send(output).unwrap();
            })
            .wait();
    }
}

#[tokio::main]
async fn main() {
    let atomic_counter = Arc::new(AtomicUsize::new(0));

    let (stdout_chan_sender, stdout_chan_receiver) = flume::unbounded();

    let (in_init_chan_sender, in_init_chan_receiver) = flume::unbounded();
    let (in_echo_chan_sender, in_echo_chan_receiver) = flume::unbounded();
    let (in_generate_chan_sender, in_generate_chan_receiver) = flume::unbounded();
    let (in_broadcast_chan_sender, in_broadcast_chan_receiver) = flume::unbounded();
    let (in_read_chan_sender, in_read_chan_receiver) = flume::unbounded();
    let (in_topology_chan_sender, in_topology_chan_receiver) = flume::unbounded();

    // We need two channels for node_id because we need to send it to two different handlers
    // And from the docs: "Note: Cloning the receiver *does not* turn this channel into a broadcast channel. Each message will only be received by a single receiver." :(
    let (node_id_for_generate_chan_sender, node_id_for_generate_chan_receiver) = flume::unbounded();
    let (node_id_for_network_chan_sender, node_id_for_network_chan_receiver) = flume::unbounded();

    task::spawn(async move {
        stdout_handler(&stdout_chan_receiver).await;
    });

    let stdout_chan_sender_clone = stdout_chan_sender.clone();
    let atomic_counter_clone = Arc::clone(&atomic_counter);

    task::spawn(async move {
        init_handler(
            in_init_chan_receiver,
            node_id_for_generate_chan_sender,
            node_id_for_network_chan_sender,
            stdout_chan_sender_clone,
            atomic_counter_clone,
        )
        .await;
    });

    let stdout_chan_sender_clone = stdout_chan_sender.clone();
    let atomic_counter_clone = Arc::clone(&atomic_counter);

    task::spawn(async move {
        echo_handler(
            in_echo_chan_receiver,
            stdout_chan_sender_clone,
            atomic_counter_clone,
        )
        .await;
    });

    let stdout_chan_sender_clone = stdout_chan_sender.clone();
    let atomic_counter_clone = Arc::clone(&atomic_counter);

    task::spawn(async move {
        generate_handler(
            in_generate_chan_receiver,
            node_id_for_generate_chan_receiver,
            stdout_chan_sender_clone,
            atomic_counter_clone,
        )
        .await;
    });

    let stdout_chan_sender_clone = stdout_chan_sender.clone();
    let atomic_counter_clone = Arc::clone(&atomic_counter);

    task::spawn(async move {
        network_handler(
            node_id_for_network_chan_receiver,
            in_topology_chan_receiver,
            in_broadcast_chan_receiver,
            in_read_chan_receiver,
            stdout_chan_sender_clone,
            atomic_counter_clone,
        )
        .await;
    });

    loop {
        let mut input = String::new();
        io::stdin().read_line(&mut input).unwrap();

        eprintln!("input: {}", input);

        let input_msg: Message<GenericBody> = serde_json::from_str(&input).unwrap();

        match input_msg.body.msg_type.as_str() {
            "init" => {
                let input_msg: Message<InitBody> = serde_json::from_str(&input).unwrap();
                in_init_chan_sender.send(input_msg).unwrap();
            }
            "echo" => {
                let input_msg: Message<EchoBody> = serde_json::from_str(&input).unwrap();
                in_echo_chan_sender.send(input_msg).unwrap();
            }
            "generate" => {
                let input_msg: Message<GenerateBody> = serde_json::from_str(&input).unwrap();
                in_generate_chan_sender.send(input_msg).unwrap();
            }
            "broadcast" => {
                let input_msg: Message<BroadcastBody> = serde_json::from_str(&input).unwrap();
                in_broadcast_chan_sender.send(input_msg).unwrap();
            }
            "read" => {
                let input_msg: Message<ReadBody> = serde_json::from_str(&input).unwrap();
                in_read_chan_sender.send(input_msg).unwrap();
            }
            "topology" => {
                let input_msg: Message<TopologyBody> = serde_json::from_str(&input).unwrap();
                in_topology_chan_sender.send(input_msg).unwrap();
            }
            "broadcast_ok" => {
                // Do nothing
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
                stdout_chan_sender.send(output).unwrap();
            }
        }
    }
}

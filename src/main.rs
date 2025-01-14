use serde::{Deserialize, Serialize};
use std::{
    collections::HashMap,
    io,
    sync::{atomic::AtomicUsize, Arc, Mutex},
};
use tokio::task;

#[derive(Serialize, Deserialize, Debug, Clone)]
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

#[derive(Serialize, Deserialize, Debug, Clone)]
struct BroadcastBody {
    #[serde(rename = "type")]
    msg_type: String,
    msg_id: Option<usize>,
    in_reply_to: Option<usize>,
    #[serde(skip_serializing_if = "Option::is_none")]
    message: Option<usize>,
    #[serde(skip_serializing_if = "Option::is_none")]
    messages: Option<Vec<usize>>,
}

#[derive(Serialize, Deserialize, Debug)]
struct BroadcastOKBody {
    #[serde(rename = "type")]
    msg_type: String,
    msg_id: Option<usize>,
    in_reply_to: Option<usize>,
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
        eprintln!("output: {}", msg)
    }
}

fn init_handler(
    stdout_chan_sender: flume::Sender<String>,
    atomic_counter: Arc<AtomicUsize>,
) -> String {
    let mut input = String::new();
    io::stdin()
        .read_line(&mut input)
        .expect("Failed to read first line");
    let input_msg: Message<InitBody> =
        serde_json::from_str(&input).expect("Failed to parse first line as init message");

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

    let output = serde_json::to_string(&output_msg).unwrap();
    stdout_chan_sender.send(output).unwrap();

    node_id
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
    node_id: String,
    in_generate_chan_receiver: flume::Receiver<Message<GenerateBody>>,
    stdout_chan_sender: flume::Sender<String>,
    atomic_counter: Arc<AtomicUsize>,
) {
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
    node_id: String,
    in_topology_chan_receiver: flume::Receiver<Message<TopologyBody>>,
    in_broadcast_chan_receiver: flume::Receiver<Message<BroadcastBody>>,
    in_broadcast_ok_chan_receiver: flume::Receiver<Message<BroadcastBody>>,
    in_read_chan_receiver: flume::Receiver<Message<ReadBody>>,
    unack_tick_receiver: flume::Receiver<()>,
    stdout_chan_sender: flume::Sender<String>,
    atomic_counter: Arc<AtomicUsize>,
) {
    let neighbors: Mutex<Vec<String>> = Mutex::new(Vec::new());
    let broadcast_messages: Mutex<Vec<usize>> = Mutex::new(Vec::new());

    // Maps used to store unack broadcast messages
    let unack_msg_id_to_values: Mutex<HashMap<usize, Vec<usize>>> = Mutex::new(HashMap::new());
    let unack_neighboor_to_value_to_msg_ids: Mutex<HashMap<String, HashMap<usize, Vec<usize>>>> =
        Mutex::new(HashMap::new());

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
                let src = input_msg.src.clone();

                let output_msg = Message {
                    src: input_msg.dest,
                    dest: input_msg.src,
                    body: BroadcastOKBody {
                        msg_type: "broadcast_ok".to_string(),
                        msg_id: Some(atomic_increment(&atomic_counter)),
                        in_reply_to: input_msg.body.msg_id,
                    },
                };

                let output = serde_json::to_string(&output_msg).unwrap();
                stdout_chan_sender.send(output).unwrap();

                let mut received_messages: Vec<usize> = Vec::new();
                if let Some(messages) = input_msg.body.messages {
                    received_messages.extend(messages);
                }
                if let Some(message) = input_msg.body.message {
                    received_messages.push(message);
                }

                let mut new_messages = received_messages.clone();
                new_messages.retain(|&x| !broadcast_messages.lock().unwrap().contains(&x));

                broadcast_messages
                    .lock()
                    .unwrap()
                    .extend(new_messages.clone());

                // If no new messages, return
                if new_messages.is_empty() {
                    return;
                }

                // broadcast to neighbors
                for neighbor in neighbors.lock().unwrap().iter() {
                    // Don't broadcast back to the sender
                    if neighbor == &src {
                        continue;
                    }

                    let output_msg = Message {
                        src: node_id.clone(),
                        dest: neighbor.clone(),
                        body: BroadcastBody {
                            msg_type: "broadcast".to_string(),
                            msg_id: Some(atomic_increment(&atomic_counter)),
                            in_reply_to: None,
                            message: None,
                            messages: Some(new_messages.clone()),
                        },
                    };

                    // Store msg_id to be able to re-broadcast until we receive broadcast_ok

                    for message in new_messages.iter() {
                        unack_msg_id_to_values
                            .lock()
                            .unwrap()
                            .entry(output_msg.body.msg_id.unwrap())
                            .or_insert(Vec::new())
                            .push(*message);

                        unack_neighboor_to_value_to_msg_ids
                            .lock()
                            .unwrap()
                            .entry(neighbor.clone())
                            .or_insert(HashMap::new())
                            .entry(*message)
                            .or_insert(Vec::new())
                            .push(output_msg.body.msg_id.unwrap());
                    }
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
            .recv(&in_broadcast_ok_chan_receiver, |input_msg| {
                let input_msg = input_msg.unwrap();

                let guard = unack_msg_id_to_values.lock().unwrap();
                let values = guard
                    .get(&input_msg.body.in_reply_to.unwrap())
                    .cloned()
                    .unwrap();
                drop(guard); // release lock

                // Remove the message id from the map
                unack_msg_id_to_values
                    .lock()
                    .unwrap()
                    .remove(&input_msg.body.in_reply_to.unwrap());

                // TODO: remove other message ids for the same value

                // Remove values from unack_neighboor_to_value_to_msg_ids
                for value in values.iter() {
                    if let Some(value_to_msg_ids) = unack_neighboor_to_value_to_msg_ids
                        .lock()
                        .unwrap()
                        .get_mut(&input_msg.src)
                    {
                        value_to_msg_ids.remove(&value);
                    }
                }

                // if neighbor has no more unack messages, remove it from the map
                unack_neighboor_to_value_to_msg_ids
                    .lock()
                    .unwrap()
                    .retain(|_, v| !v.is_empty());
            })
            .recv(&unack_tick_receiver, |_| {
                let mut msg_ids_to_insert: Vec<(String, usize, usize)> = Vec::new();
                let mut msgs_to_send: Vec<Message<BroadcastBody>> = Vec::new();

                // Resend all unack broadcast messages
                for (neighbor, value_to_msg_ids) in
                    unack_neighboor_to_value_to_msg_ids.lock().unwrap().iter()
                {
                    let values = value_to_msg_ids.keys().cloned().collect::<Vec<usize>>();

                    let output_msg = Message {
                        src: node_id.clone(),
                        dest: neighbor.clone(),
                        body: BroadcastBody {
                            msg_type: "broadcast".to_string(),
                            msg_id: Some(atomic_increment(&atomic_counter)),
                            in_reply_to: None,
                            message: None,
                            messages: Some(values.clone()),
                        },
                    };

                    msgs_to_send.push(output_msg.clone());

                    for value in values {
                        msg_ids_to_insert.push((
                            neighbor.clone(),
                            value,
                            output_msg.body.msg_id.unwrap(),
                        ));
                    }
                }

                for (neighbor, value, msg_id) in msg_ids_to_insert {
                    unack_neighboor_to_value_to_msg_ids
                        .lock()
                        .unwrap()
                        .entry(neighbor)
                        .or_insert(HashMap::new())
                        .entry(value)
                        .or_insert(Vec::new())
                        .push(msg_id);

                    unack_msg_id_to_values
                        .lock()
                        .unwrap()
                        .entry(msg_id)
                        .or_insert(Vec::new())
                        .push(value);
                }

                for msg in msgs_to_send.iter() {
                    let output = serde_json::to_string(&msg).unwrap();
                    stdout_chan_sender.send(output).unwrap();
                }
            })
            .wait();
    }
}

#[tokio::main]
async fn main() {
    let atomic_counter = Arc::new(AtomicUsize::new(0));

    let (stdout_chan_sender, stdout_chan_receiver) = flume::unbounded();

    let (in_echo_chan_sender, in_echo_chan_receiver) = flume::unbounded();
    let (in_generate_chan_sender, in_generate_chan_receiver) = flume::unbounded();
    let (in_broadcast_chan_sender, in_broadcast_chan_receiver) = flume::unbounded();
    let (in_read_chan_sender, in_read_chan_receiver) = flume::unbounded();
    let (in_topology_chan_sender, in_topology_chan_receiver) = flume::unbounded();
    let (in_broadcast_ok_chan_sender, in_broadcast_ok_chan_receiver) = flume::unbounded();

    let (unac_tick_sender, unack_tick_receiver) = flume::unbounded();

    // The first message is always an init message, containing the node_id
    let node_id = init_handler(stdout_chan_sender.clone(), Arc::clone(&atomic_counter));

    task::spawn(async move {
        stdout_handler(&stdout_chan_receiver).await;
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
    let node_id_clone = node_id.clone();

    task::spawn(async move {
        generate_handler(
            node_id_clone,
            in_generate_chan_receiver,
            stdout_chan_sender_clone,
            atomic_counter_clone,
        )
        .await;
    });

    let stdout_chan_sender_clone = stdout_chan_sender.clone();
    let atomic_counter_clone = Arc::clone(&atomic_counter);
    let node_id_clone = node_id.clone();

    task::spawn(async move {
        network_handler(
            node_id_clone,
            in_topology_chan_receiver,
            in_broadcast_chan_receiver,
            in_broadcast_ok_chan_receiver,
            in_read_chan_receiver,
            unack_tick_receiver,
            stdout_chan_sender_clone,
            atomic_counter_clone,
        )
        .await;
    });

    // Periodically send a tick, which will trigger unack broadcast to be resent
    tokio::spawn(async move {
        loop {
            unac_tick_sender.send(()).unwrap();
            tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;
        }
    });

    loop {
        let mut input = String::new();
        io::stdin().read_line(&mut input).unwrap();

        eprintln!("input: {}", input);

        let input_msg: Message<GenericBody> = serde_json::from_str(&input).unwrap();

        match input_msg.body.msg_type.as_str() {
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
                let input_msg: Message<BroadcastBody> = serde_json::from_str(&input).unwrap();
                in_broadcast_ok_chan_sender.send(input_msg).unwrap();
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

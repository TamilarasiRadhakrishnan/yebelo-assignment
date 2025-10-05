use rdkafka::config::ClientConfig;
use rdkafka::consumer::{StreamConsumer, Consumer};
use rdkafka::message::Message;
use rdkafka::producer::{FutureProducer, FutureRecord};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::time::Duration;

#[derive(Deserialize)]
struct Trade {
    token_address: String,
    price_in_sol: f64,
    block_time: Option<String>,
}

#[derive(Serialize)]
struct RsiMsg {
    token_address: String,
    rsi: f64,
    timestamp: String,
}

#[tokio::main]
async fn main() {
    let brokers = std::env::var("KAFKA_BROKERS").unwrap_or_else(|_| "localhost:9092".into());
    let consumer: StreamConsumer = ClientConfig::new()
        .set("bootstrap.servers", &brokers)
        .set("group.id", "rsi-group")
        .create()
        .expect("Consumer creation failed");

    consumer.subscribe(&["trade-data"]).expect("subscribe failed");

    let producer: FutureProducer = ClientConfig::new()
        .set("bootstrap.servers", &brokers)
        .create()
        .expect("Producer creation failed");

    let mut windows: HashMap<String, Vec<f64>> = HashMap::new();
    const WINDOW: usize = 14;

    println!("RSI consumer started (brokers={})", brokers);

    use futures::StreamExt;
    let mut stream = consumer.stream();
    while let Some(msg) = stream.next().await {
        match msg {
            Ok(m) => {
                if let Some(payload) = m.payload_view::<str>().ok().flatten() {
                    if let Ok(trade) = serde_json::from_str::<Trade>(payload) {
                        let vx = windows.entry(trade.token_address.clone()).or_default();
                        vx.push(trade.price_in_sol);
                        if vx.len() > WINDOW { vx.remove(0); }
                        if vx.len() == WINDOW {
                            let rsi = calc_rsi(&vx);
                            let rsi_msg = RsiMsg {
                                token_address: trade.token_address.clone(),
                                rsi,
                                timestamp: trade.block_time.unwrap_or_else(|| chrono::Utc::now().to_rfc3339()),
                            };
                            let json = serde_json::to_string(&rsi_msg).unwrap();
                            let _ = producer.send(
                                FutureRecord::to("rsi-data").payload(&json).key(&trade.token_address),
                                Duration::from_secs(0),
                            ).await;
                            println!("Published RSI for {}: {:.2}", trade.token_address, rsi);
                        }
                    }
                }
            }
            Err(e) => eprintln!("Kafka error: {}", e),
        }
    }
}

fn calc_rsi(prices: &Vec<f64>) -> f64 {
    if prices.len() < 2 { return 50.0; }
    let mut gains = 0.0;
    let mut losses = 0.0;
    for i in 1..prices.len() {
        let diff = prices[i] - prices[i-1];
        if diff >= 0.0 { gains += diff; } else { losses += -diff; }
    }
    let periods = (prices.len() - 1) as f64;
    let avg_gain = gains / periods;
    let avg_loss = losses / periods;
    if avg_loss == 0.0 { return 100.0; }
    let rs = avg_gain / avg_loss;
    100.0 - (100.0 / (1.0 + rs))
}

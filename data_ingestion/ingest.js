import fs from "fs";
import csv from "csv-parser";
import { Kafka } from "kafkajs";

const kafka = new Kafka({
  clientId: "trade-ingestion",
  brokers: ["localhost:9092"], // Redpanda Kafka port
});

const producer = kafka.producer();

async function sendTrades() {
  await producer.connect();

  const topic = "trades";

  fs.createReadStream("../trades_data.csv")
    .pipe(csv())
    .on("data", async (row) => {
      try {
        await producer.send({
          topic,
          messages: [{ value: JSON.stringify(row) }],
        });
        console.log("Sent trade:", row);
      } catch (err) {
        console.error("Error sending:", err);
      }
    })
    .on("end", async () => {
      console.log("✅ All trades sent successfully!");
      await producer.disconnect();
    });
}

sendTrades().catch(console.error);

import { Kafka } from "kafkajs";
import WebSocket, { WebSocketServer } from "ws";

const brokers = [process.env.KAFKA_BROKER || "localhost:9092"];
const kafka = new Kafka({ brokers });

const consumer = kafka.consumer({ groupId: "ws-bridge-group" });

async function main() {
  await consumer.connect();
  await consumer.subscribe({ topic: "rsi-data", fromBeginning: false });

  const wss = new WebSocketServer({ port: process.env.WS_PORT ? parseInt(process.env.WS_PORT) : 4000 });
  console.log(`WS bridge running on ws://localhost:${process.env.WS_PORT || 4000}`);

  consumer.run({
    eachMessage: async ({ message }) => {
      const payload = message.value?.toString();
      if (!payload) return;
      wss.clients.forEach(client => {
        if (client.readyState === WebSocket.OPEN) client.send(payload);
      });
    }
  });
}

main().catch(err => { console.error(err); process.exit(1); });

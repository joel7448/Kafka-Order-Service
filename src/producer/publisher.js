import { kafka, registerGracefulShutdown } from "../config/kafka.js";
import { TOPICS } from "../config/constants.js";

const producer = kafka.producer();
registerGracefulShutdown({ producer });

async function run() {
  await producer.connect();
  console.log("[Producer] Connected. Publishing sample batch...");

  const sampleEvents = [
    { orderId: "ord-101", amount: 99.99, action: "NORMAL" },
    { orderId: "ord-102", amount: 45.0, action: "TRANSIENT_FLAKE" },
    { orderId: "ord-103", amount: 15.5, action: "POISON_PILL" },
    { orderId: "ord-104", amount: 250.0, action: "TRANSIENT_FLAKE" },
  ];

  await producer.send({
    topic: TOPICS.ORDERS,
    messages: sampleEvents.map((order) => ({
      key: order.orderId, // Key ensures partition stickiness per order
      value: JSON.stringify(order),
      headers: {
        "x-origin-client": Buffer.from("api-gateway"),
        "x-created-at": Buffer.from(Date.now().toString()),
      },
    })),
  });

  console.log("[Producer] Messages emitted. Exiting producer process.");
  await producer.disconnect();
  process.exit(0);
}

run().catch(console.error);

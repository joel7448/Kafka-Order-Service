import { Kafka, logLevel } from "kafkajs";

export const kafka = new Kafka({
  clientId: process.env.KAFKA_CLIENT_ID || "order-system",
  brokers: (process.env.KAFKA_BROKERS || "localhost:9092").split(","),
  logLevel: logLevel.NOTHING,
  retry: {
    initialRetryTime: 300,
    retries: 8,
  },
});

export function registerGracefulShutdown({ producer, consumer }) {
  const shutdown = async (signal) => {
    console.log(`\nReceived ${signal}. Gracefully stopping...`);
    try {
      if (consumer) await consumer.disconnect();
      if (producer) await producer.disconnect();
    } finally {
      process.exit(0);
    }
  };

  process.on("SIGINT", () => shutdown("SIGINT"));
  process.on("SIGTERM", () => shutdown("SIGTERM"));
}

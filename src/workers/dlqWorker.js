import { kafka, registerGracefulShutdown } from "../config/kafka.js";
import { TOPICS, CONSUMER_GROUPS } from "../config/constants.js";

const consumer = kafka.consumer({ groupId: CONSUMER_GROUPS.DLQ });
registerGracefulShutdown({ consumer });

async function run() {
  await consumer.connect();
  await consumer.subscribe({ topic: TOPICS.ORDERS_DLQ, fromBeginning: false });

  console.log(
    `[DLQWorker] Monitoring dead letters on "${TOPICS.ORDERS_DLQ}"...`,
  );

  await consumer.run({
    autoCommit: false,
    eachMessage: async ({ topic, partition, message }) => {
      const order = JSON.parse(message.value.toString());
      const reason =
        message.headers["x-dead-letter-reason"]?.toString() || "Unknown";
      const failedAt = message.headers["x-failed-at"]?.toString();

      // Sinks to an alert dashboard, PagerDuty, or long-term cold store (S3/PostgreSQL)
      console.error("================ DEAD LETTER RECEIVED ================");
      console.error(`Order ID  : ${order.orderId}`);
      console.error(`Reason    : ${reason}`);
      console.error(
        `Failed At : ${failedAt ? new Date(parseInt(failedAt, 10)).toISOString() : "N/A"}`,
      );
      console.error("Payload   :", order);
      console.error("======================================================");

      await consumer.commitOffsets([
        { topic, partition, offset: (BigInt(message.offset) + 1n).toString() },
      ]);
    },
  });
}

run().catch(console.error);

import { kafka, registerGracefulShutdown } from "../config/kafka.js";
import { TOPICS, CONSUMER_GROUPS } from "../config/constants.js";
import { buildRetryHeaders } from "../utils/headers.js";
import { processOrder } from "../services/orderService.js";

const consumer = kafka.consumer({ groupId: CONSUMER_GROUPS.MAIN });
const producer = kafka.producer(); // Producer needed for forwarding failures
registerGracefulShutdown({ consumer, producer });

async function run() {
  await producer.connect();
  await consumer.connect();
  await consumer.subscribe({ topic: TOPICS.ORDERS, fromBeginning: false });

  console.log(`[MainWorker] Listening on "${TOPICS.ORDERS}"...`);

  await consumer.run({
    autoCommit: false,
    eachMessage: async ({ topic, partition, message }) => {
      const order = JSON.parse(message.value.toString());

      try {
        await processOrder(order);
      } catch (err) {
        console.warn(
          `[MainWorker] Failed ${order.orderId}: ${err.message}. Routing to retry...`,
        );

        // Forward to retry topic with retry counter initialized to 1
        await producer.send({
          topic: TOPICS.ORDERS_RETRY,
          messages: [
            {
              key: message.key,
              value: message.value,
              headers: buildRetryHeaders(message.headers, 1, err),
            },
          ],
        });
      }

      // Always commit on main topic to prevent head-of-line blocking
      await consumer.commitOffsets([
        { topic, partition, offset: (BigInt(message.offset) + 1n).toString() },
      ]);
    },
  });
}

run().catch(console.error);

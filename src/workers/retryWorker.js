import { kafka, registerGracefulShutdown } from "../config/kafka.js";
import { TOPICS, CONSUMER_GROUPS, RETRY_CONFIG } from "../config/constants.js";
import { getHeaderInt, buildRetryHeaders } from "../utils/headers.js";
import { processOrder } from "../services/orderService.js";

const consumer = kafka.consumer({ groupId: CONSUMER_GROUPS.RETRY });
const producer = kafka.producer();
registerGracefulShutdown({ consumer, producer });

async function run() {
  await producer.connect();
  await consumer.connect();
  await consumer.subscribe({
    topic: TOPICS.ORDERS_RETRY,
    fromBeginning: false,
  });

  console.log(`[RetryWorker] Listening on "${TOPICS.ORDERS_RETRY}"...`);

  await consumer.run({
    autoCommit: false,
    eachMessage: async ({ topic, partition, message }) => {
      const order = JSON.parse(message.value.toString());
      const attempt = getHeaderInt(message.headers, "x-retry-count", 1);

      // Exponential backoff delay: 2s, 4s, 8s
      const delayMs = RETRY_CONFIG.BASE_DELAY_MS * Math.pow(2, attempt - 1);
      console.log(
        `[RetryWorker] Backoff ${delayMs}ms before attempt #${attempt} for ${order.orderId}`,
      );
      await new Promise((r) => setTimeout(r, delayMs));

      try {
        await processOrder(order);
      } catch (err) {
        if (attempt < RETRY_CONFIG.MAX_ATTEMPTS) {
          console.warn(
            `[RetryWorker] Attempt #${attempt} failed. Re-queueing...`,
          );
          await producer.send({
            topic: TOPICS.ORDERS_RETRY,
            messages: [
              {
                key: message.key,
                value: message.value,
                headers: buildRetryHeaders(message.headers, attempt + 1, err),
              },
            ],
          });
        } else {
          console.error(
            `[RetryWorker] Max attempts (${RETRY_CONFIG.MAX_ATTEMPTS}) reached. Forwarding to DLQ.`,
          );
          await producer.send({
            topic: TOPICS.ORDERS_DLQ,
            messages: [
              {
                key: message.key,
                value: message.value,
                headers: {
                  ...message.headers,
                  "x-dead-letter-reason": Buffer.from(err.message),
                  "x-routed-at": Buffer.from(Date.now().toString()),
                },
              },
            ],
          });
        }
      }

      await consumer.commitOffsets([
        { topic, partition, offset: (BigInt(message.offset) + 1n).toString() },
      ]);
    },
  });
}

run().catch(console.error);

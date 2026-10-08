import readline from "readline";
import { kafka } from "../config/kafka.js";
import { TOPICS } from "../config/constants.js";

// Dedicated consumer group so the replay tool does not collide with audit/monitoring workers
const REPLAY_GROUP = "orders-dlq-manual-replay-tool";

const consumer = kafka.consumer({ groupId: REPLAY_GROUP });
const producer = kafka.producer();

const rl = readline.createInterface({
  input: process.stdin,
  output: process.stdout,
});

const askQuestion = (query) =>
  new Promise((resolve) => rl.question(query, resolve));

/**
 * Cleans retry/failure headers and stamps replay provenance
 */
function sanitizeHeadersForReplay(headers = {}) {
  const sanitized = { ...headers };

  // Remove failure traces so workers process cleanly
  delete sanitized["x-retry-count"];
  delete sanitized["x-error-message"];
  delete sanitized["x-failed-at"];
  delete sanitized["x-dead-letter-reason"];
  delete sanitized["x-routed-at"];

  // Add audit trail headers
  sanitized["x-replayed-at"] = Buffer.from(Date.now().toString());
  sanitized["x-replayed-from"] = Buffer.from(TOPICS.ORDERS_DLQ);

  return sanitized;
}

async function run() {
  console.log("=== Kafka DLQ Replay Utility ===\n");

  await producer.connect();
  await consumer.connect();

  // Read unread messages from current committed offset of this tool's group
  await consumer.subscribe({ topic: TOPICS.ORDERS_DLQ, fromBeginning: false });

  let isRunning = true;

  process.on("SIGINT", async () => {
    console.log("\nExiting replay utility...");
    isRunning = false;
    await consumer.disconnect();
    await producer.disconnect();
    rl.close();
    process.exit(0);
  });

  await consumer.run({
    autoCommit: false,
    eachMessage: async ({ topic, partition, message }) => {
      if (!isRunning) return;

      const payload = JSON.parse(message.value.toString());
      const key = message.key ? message.key.toString() : "NO_KEY";
      const reason =
        message.headers["x-dead-letter-reason"]?.toString() || "Unknown";
      const failedAt = message.headers["x-failed-at"]?.toString();

      console.log("\n----------------------------------------");
      console.log(`[P${partition} | Offset: ${message.offset}]`);
      console.log(`Key        : ${key}`);
      console.log(`Failure    : ${reason}`);
      console.log(
        `Failed At  : ${failedAt ? new Date(parseInt(failedAt, 10)).toISOString() : "N/A"}`,
      );
      console.log("Payload    :", JSON.stringify(payload, null, 2));
      console.log("----------------------------------------");

      // Prompt operator for action
      const answer = await askQuestion(
        "Action? [r]eplay back to main / [s]kip and discard / [q]uit: ",
      );

      const action = answer.trim().toLowerCase();

      if (action === "r") {
        const replayHeaders = sanitizeHeadersForReplay(message.headers);

        await producer.send({
          topic: TOPICS.ORDERS,
          messages: [
            {
              key: message.key,
              value: message.value,
              headers: replayHeaders,
            },
          ],
        });

        console.log(`✔ Re-published ${key} to "${TOPICS.ORDERS}"`);

        // Advance DLQ cursor past this processed record
        await consumer.commitOffsets([
          {
            topic,
            partition,
            offset: (BigInt(message.offset) + 1n).toString(),
          },
        ]);
      } else if (action === "s") {
        console.log(`Skipped ${key}. Marking as consumed.`);

        // Acknowledge/discard without republishing
        await consumer.commitOffsets([
          {
            topic,
            partition,
            offset: (BigInt(message.offset) + 1n).toString(),
          },
        ]);
      } else if (action === "q") {
        console.log("Aborting replay session...");
        isRunning = false;
        await consumer.disconnect();
        await producer.disconnect();
        rl.close();
        process.exit(0);
      } else {
        console.log("Invalid option. Message left uncommitted in DLQ.");
      }
    },
  });
}

run().catch(console.error);

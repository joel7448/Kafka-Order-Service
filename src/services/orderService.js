/**
 * Isolated business logic - zero Kafka dependencies.
 * Easily unit tested with mocks.
 */
export async function processOrder(order) {
  if (order.action === "POISON_PILL") {
    throw new Error("Fatal: Invalid schema or unparseable customer ID");
  }

  if (order.action === "TRANSIENT_FLAKE" && Math.random() < 0.6) {
    throw new Error("Transient: Payment gateway socket timeout (504)");
  }

  console.log(
    `[OrderService] Success: Processed order ${order.orderId} ($${order.amount})`,
  );
}

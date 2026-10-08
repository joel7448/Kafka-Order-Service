export const TOPICS = {
  ORDERS: "orders",
  ORDERS_RETRY: "orders-retry",
  ORDERS_DLQ: "orders-dlq",
};

export const CONSUMER_GROUPS = {
  MAIN: "orders-main-processor-group",
  RETRY: "orders-retry-processor-group",
  DLQ: "orders-dlq-audit-group",
};

export const RETRY_CONFIG = {
  MAX_ATTEMPTS: 3,
  BASE_DELAY_MS: 2000,
};

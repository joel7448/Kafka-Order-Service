export function getHeaderInt(headers, key, defaultValue = 0) {
  if (!headers || !headers[key]) return defaultValue;
  const val = parseInt(headers[key].toString(), 10);
  return Number.isNaN(val) ? defaultValue : val;
}

export function buildRetryHeaders(originalHeaders = {}, nextCount, error) {
  return {
    ...originalHeaders,
    'x-retry-count': Buffer.from(nextCount.toString()),
    'x-error-message': Buffer.from(error.message || 'Unknown error'),
    'x-failed-at': Buffer.from(Date.now().toString()),
  };
}
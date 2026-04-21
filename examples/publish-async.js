// Example: async publish with broker acknowledgement (AMQP 0.9.1).
//
// publishAsync() returns a Promise<void> that resolves when the underlying
// channel write to the broker completes without error. Use it with
// `async default()` and `await` to block the VU until the broker accepts
// the message before moving on.
//
// Note: publishAsync opens a new AMQP channel per call. It does NOT enable
// full publisher-confirm mode (ch.Confirm) — the promise resolves when the
// channel write succeeds, not when the broker persists the message to disk.
// For strict at-least-once guarantees, use a broker with quorum queues and
// persistent messages alongside this pattern.
//
// Run with:
//   k6 run examples/publish-async.js
import { Client } from 'k6/x/amqp091';
import { sleep } from 'k6';

export const options = {
  vus: 5,
  duration: '30s',
};

const client = new Client({
  connectionOptions: { host: 'localhost', port: 5672, username: 'guest', password: 'guest' },
});

const EXCHANGE = 'k6-async-exchange';
const QUEUE    = 'k6-async-queue';

export function setup() {
  client.declareExchange({ name: EXCHANGE, kind: 'direct', durable: true });
  client.declareQueue({ name: QUEUE, durable: true });
  client.bindQueue({ queueName: QUEUE, exchangeName: EXCHANGE, routingKey: 'async' });
}

// default() must be async to use await with publishAsync.
export default async function () {
  await client.publishAsync({
    exchange: EXCHANGE,
    routingKey: 'async',
    body: `Confirmed message from VU ${__VU} iter ${__ITER}`,
    contentType: 'text/plain',
    persistent: true,
  });

  console.log(`VU ${__VU}: message accepted by broker`);
  sleep(0.05);
}

export function teardown() {
  client.purgeQueue(QUEUE);
  client.deleteQueue(QUEUE);
  client.deleteExchange(EXCHANGE);
  client.close();
}

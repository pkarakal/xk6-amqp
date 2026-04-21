// Example: inspect queue metadata (AMQP 0.9.1).
//
// inspectQueue(name) returns { name, messages, consumers } without modifying
// the queue. Internally it uses QueueDeclarePassive — the queue must already exist.
//
// Run with:
//   k6 run examples/inspect-queue.js
import { Client } from 'k6/x/amqp091';
import { sleep } from 'k6';

export const options = {
  vus: 1,
  iterations: 3,
};

const client = new Client({
  connectionOptions: { host: 'localhost', port: 5672, username: 'guest', password: 'guest' },
});

export function setup() {
  client.declareQueue({ name: 'k6-queue', durable: false });

  // Pre-populate with a few messages so inspectQueue reports non-zero counts.
  for (let i = 0; i < 5; i++) {
    client.publish({
      queueName: 'k6-queue',
      body: `msg ${i}`,
      contentType: 'text/plain',
    });
  }
}

export default function () {
  // Returns { name: string, messages: number, consumers: number }
  const info = client.inspectQueue('k6-queue');
  console.log(JSON.stringify(info, null, 2));
  sleep(0.5);
}

export function teardown() {
  client.purgeQueue('k6-queue');
  client.deleteQueue('k6-queue');
  client.close();
}

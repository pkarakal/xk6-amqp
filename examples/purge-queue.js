// Example: purge all messages from a queue (AMQP 0.9.1).
//
// purgeQueue(name) removes all messages and returns the count of messages purged.
//
// Run with:
//   k6 run examples/purge-queue.js
import { Client } from 'k6/x/amqp091';
import { sleep } from 'k6';

export const options = {
  vus: 1,
  iterations: 1,
};

const client = new Client({
  connectionOptions: { host: 'localhost', port: 5672, username: 'guest', password: 'guest' },
});

export function setup() {
  client.declareQueue({ name: 'k6-queue', durable: false });

  // Pre-populate the queue with messages to purge.
  for (let i = 0; i < 10; i++) {
    client.publish({
      queueName: 'k6-queue',
      body: `msg ${i}`,
      contentType: 'text/plain',
    });
  }
}

export default function () {
  // Returns the number of messages removed.
  const count = client.purgeQueue('k6-queue');
  console.log(`Purged ${count} messages from k6-queue`);
  sleep(0);
}

export function teardown() {
  client.deleteQueue('k6-queue');
  client.close();
}

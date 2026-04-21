// Example: delete a queue (AMQP 0.9.1).
// The script is self-contained: it declares the queue first so it can be run
// against any broker without pre-existing resources.
//
// Run with:
//   k6 run examples/delete-queue.js
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
  client.deleteQueue('k6-queue');
  console.log('k6-queue deleted');
}

export default function () {
  sleep(0);
}

export function teardown() {
  client.close();
}

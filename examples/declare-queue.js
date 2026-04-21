// Example: declare a queue with all available options (AMQP 0.9.1).
// Use this as a reference card for declareQueue parameters.
//
// Key rename from legacy API: delete_when_unused → deleteWhenUnused (camelCase).
//
// Run with:
//   k6 run examples/declare-queue.js
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
  client.declareQueue({
    name: 'k6-queue',
    durable: true,           // survive broker restarts
    deleteWhenUnused: false, // do not delete when last consumer unsubscribes
    exclusive: false,        // allow multiple connections to access the queue
    noWait: false,           // wait for broker confirmation
    // args: { 'x-message-ttl': 60000 },  // optional: message TTL in ms
  });

  console.log('k6-queue declared');
}

export default function () {
  sleep(0);
}

export function teardown() {
  client.deleteQueue('k6-queue');
  client.close();
}

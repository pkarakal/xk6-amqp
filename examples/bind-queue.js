// Example: bind a queue to an exchange (AMQP 0.9.1).
//
// AMQP 0.9.1 bindQueue parameter names:
//   { queueName, exchangeName, routingKey, noWait?, args? }
//
// AMQP 1.0 bindQueue parameter names (different!):
//   { sourceExchange, destinationQueue, bindingKey?, args? }
//   — and bindQueue returns a binding path string required by unbindQueue.
//
// For AMQP 0.9.1 bindQueue returns '' (empty string). Use unbindQueueWithOptions
// to remove the binding — unbindQueue(path) always throws for 0.9.1.
//
// Run with:
//   k6 run examples/bind-queue.js
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
  client.declareExchange({ name: 'k6-exchange', kind: 'direct', durable: false });
  client.declareQueue({ name: 'k6-queue', durable: false });

  // Returns '' for AMQP 0.9.1 (no binding path concept in the protocol).
  const bindResult = client.bindQueue({
    queueName: 'k6-queue',
    exchangeName: 'k6-exchange',
    routingKey: 'k6.test',
    noWait: false,
    // args: null,
  });

  console.log(`k6-queue bound to k6-exchange (result: '${bindResult}')`);
}

export default function () {
  sleep(0);
}

export function teardown() {
  client.deleteQueue('k6-queue');
  client.deleteExchange('k6-exchange');
  client.close();
}

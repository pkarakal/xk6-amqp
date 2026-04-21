// Example: remove a queue-to-exchange binding (AMQP 0.9.1).
//
// IMPORTANT: For AMQP 0.9.1, use unbindQueueWithOptions({ queueName, exchangeName, routingKey }).
// client.unbindQueue(path) always throws for 0.9.1 because the protocol has no binding path concept.
//
// For AMQP 1.0, use unbindQueue(bindingPath) where bindingPath is the string
// returned by bindQueue({ sourceExchange, destinationQueue, bindingKey }).
//
// Run with:
//   k6 run examples/unbind-queue.js
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
  client.bindQueue({ queueName: 'k6-queue', exchangeName: 'k6-exchange', routingKey: 'k6' });

  // AMQP 0.9.1: use unbindQueueWithOptions (must match the binding exactly).
  client.unbindQueueWithOptions({
    queueName: 'k6-queue',
    exchangeName: 'k6-exchange',
    routingKey: 'k6',
    // args: null,
  });

  console.log('k6-queue unbound from k6-exchange');
}

export default function () {
  sleep(0);
}

export function teardown() {
  client.deleteQueue('k6-queue');
  client.deleteExchange('k6-exchange');
  client.close();
}

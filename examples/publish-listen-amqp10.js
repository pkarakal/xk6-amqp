// Example: publish and listen using AMQP 1.0 (RabbitMQ with AMQP 1.0 plugin).
// The API shape is identical to k6/x/amqp091; swap the import to switch protocols.
import { Client } from 'k6/x/amqp10';

export const options = {
  stages: [
    { duration: '3m', target: 10 },
    { duration: '5m', target: 10 },
    { duration: '10m', target: 35 },
    { duration: '3m', target: 0 },
  ],
};

// Clients are created in the init context (no IO yet) and connected lazily.
const client = new Client({
  connectionOptions: {
    host: 'localhost',
    port: 5672,
    username: 'guest',
    password: 'guest',
  },
});

export function setup() {
  // Declare resources once before the test starts.
  client.declareExchange({ name: 'k6-exchange', kind: 'direct' });
  client.declareQueue({ name: 'k6-queue', queueType: 'classic' });

  // bindQueue returns a binding path required by unbindQueue.
  const bindingPath = client.bindQueue({
    sourceExchange: 'k6-exchange',
    destinationQueue: 'k6-queue',
    bindingKey: 'k6',
  });
  console.log('Binding path:', bindingPath);

  // Start a listener in the background.
  client.listen({
    queueName: 'k6-queue',
    autoAck: true,
    listener: (msg) => {
      console.log('Received:', msg);
    },
  });
}

export default function () {
  client.publish({
    exchange: 'k6-exchange',
    routingKey: 'k6',
    contentType: 'text/plain',
    body: 'Ping from k6 via AMQP 1.0',
  });
}

export function teardown() {
  client.deleteQueue('k6-queue');
  client.deleteExchange('k6-exchange');
  client.close();
}

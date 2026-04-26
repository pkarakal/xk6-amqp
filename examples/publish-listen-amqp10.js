// Example: publish and listen using AMQP 1.0 (RabbitMQ with AMQP 1.0 plugin).
// The API shape is identical to k6/x/amqp091; swap the import to switch protocols.
import { Client } from 'k6/x/amqp10';
import { sleep } from 'k6';

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

// bindQueue returns a binding path for AMQP 1.0 — store it for teardown.
let bindingPath;

export function setup() {
  // Declare resources once before the test starts.
  client.declareExchange({ name: 'k6-exchange', kind: 'direct' });
  client.declareQueue({ name: 'k6-queue', queueType: 'classic' });

  // bindQueue returns a binding path required by unbindQueue.
  bindingPath = client.bindQueue({
    sourceExchange: 'k6-exchange',
    destinationQueue: 'k6-queue',
    bindingKey: 'k6',
  });
  console.log('Binding path:', bindingPath);
}

// Per-VU flag: each VU calls listen() exactly once on its first iteration.
// listen() must NOT be called in setup() — the setup VU's event loop does not
// run during the test phase, so the listener callback would never fire.
let _listening = false;

export default function () {
  if (!_listening) {
    client.listen({
      queueName: 'k6-queue',
      autoAck: true,
      listener: (msg) => {
        console.log('Received:', msg.body);
      },
    });
    _listening = true;
  }

  client.publish({
    exchange: 'k6-exchange',
    routingKey: 'k6',
    contentType: 'text/plain',
    body: 'Ping from k6 via AMQP 1.0',
  });

  sleep(0.01);
}

export function teardown() {
  if (bindingPath) {
    client.unbindQueue(bindingPath);
  }
  client.deleteQueue('k6-queue');
  client.deleteExchange('k6-exchange');
  client.close();
}

// Example: quorum queue publish and consume (AMQP 1.0).
//
// Quorum queues are replicated, crash-safe queues backed by the Raft consensus
// algorithm (requires RabbitMQ 3.8+). They offer stronger durability guarantees
// than classic queues at the cost of higher write latency.
//
// Key constraints:
//   - queueType: 'quorum' maps to QuorumQueueSpecification in the AMQP 1.0 layer.
//   - Quorum queues cannot be exclusive or autoDelete.
//   - AMQP 1.0 bindQueue returns a binding path string — save it for unbindQueue.
//   - initialCredits controls AMQP 1.0 link flow (prefetch); default is 256.
//
// Requires: RabbitMQ with AMQP 1.0 plugin enabled.
//   rabbitmq-plugins enable rabbitmq_amqp1_0
//
// Run with:
//   k6 run examples/quorum-queue.js
import { Client } from 'k6/x/amqp10';
import { sleep } from 'k6';

export const options = {
  vus: 3,
  duration: '20s',
};

const client = new Client({
  connectionOptions: { host: 'localhost', port: 5672, username: 'guest', password: 'guest' },
});

const EXCHANGE = 'k6-quorum-exchange';
const QUEUE    = 'k6-quorum-queue';

// bindQueue returns a binding path for AMQP 1.0 — store it for teardown.
let bindingPath;

export function setup() {
  client.declareExchange({ name: EXCHANGE, kind: 'direct' });

  // queueType: 'quorum' — replicated, crash-safe. Cannot be exclusive or autoDelete.
  client.declareQueue({ name: QUEUE, queueType: 'quorum' });

  // AMQP 1.0: bindQueue returns the binding path required by unbindQueue.
  bindingPath = client.bindQueue({
    sourceExchange: EXCHANGE,
    destinationQueue: QUEUE,
    bindingKey: 'quorum',
  });
  console.log('Binding path:', bindingPath);
}

let _listening = false;

export default function () {
  if (!_listening) {
    client.listen({
      queueName: QUEUE,
      autoAck: true,
      initialCredits: 50,  // AMQP 1.0 flow control: prefetch up to 50 messages
      listener: (msg) => { console.log(`Quorum queue received: ${msg}`); },
    });
    _listening = true;
  }

  client.publish({
    exchange: EXCHANGE,
    routingKey: 'quorum',
    body: `Durable message from VU ${__VU} iter ${__ITER}`,
    contentType: 'text/plain',
    persistent: true,
  });

  sleep(0.1);
}

export function teardown() {
  if (bindingPath) {
    client.unbindQueue(bindingPath);
  }
  client.purgeQueue(QUEUE);
  client.deleteQueue(QUEUE);
  client.deleteExchange(EXCHANGE);
  client.close();
}

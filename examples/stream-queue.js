// Example: stream queue publish and consume (AMQP 1.0).
//
// Stream queues are append-only, persistent logs with configurable retention
// (requires RabbitMQ 3.9+ with the rabbitmq-stream plugin). Unlike classic or
// quorum queues, messages are never removed by consumer acknowledgements —
// retention is controlled by size and time limits set via args.
//
// Key constraints:
//   - queueType: 'stream' requires the rabbitmq-stream plugin.
//   - autoDelete is not supported on stream queues.
//   - Do NOT purgeQueue on a stream — it truncates by offset, not by ack.
//   - initialCredits matters more for streams: the broker delivers in bulk.
//   - AMQP 1.0 bindQueue returns a binding path string — save it for unbindQueue.
//
// Requires: RabbitMQ with AMQP 1.0 and stream plugin enabled.
//   rabbitmq-plugins enable rabbitmq_amqp1_0 rabbitmq_stream
//
// Run with:
//   k6 run examples/stream-queue.js
import { Client } from 'k6/x/amqp10';
import { sleep } from 'k6';

export const options = {
  vus: 3,
  duration: '20s',
};

const client = new Client({
  connectionOptions: { host: 'localhost', port: 5672, username: 'guest', password: 'guest' },
});

const EXCHANGE = 'k6-stream-exchange';
const QUEUE    = 'k6-stream-queue';

// bindQueue returns a binding path for AMQP 1.0 — store it for teardown.
let bindingPath;

export function setup() {
  client.declareExchange({ name: EXCHANGE, kind: 'direct' });

  // queueType: 'stream' — append-only log with size-based retention.
  client.declareQueue({
    name: QUEUE,
    queueType: 'stream',
    args: {
      'x-max-length-bytes':              100_000_000,  // 100 MB total retention
      'x-stream-max-segment-size-bytes':  20_000_000,  // 20 MB per segment file
    },
  });

  bindingPath = client.bindQueue({
    sourceExchange: EXCHANGE,
    destinationQueue: QUEUE,
    bindingKey: 'stream',
  });
  console.log('Binding path:', bindingPath);
}

let _listening = false;

export default function () {
  if (!_listening) {
    client.listen({
      queueName: QUEUE,
      autoAck: true,
      initialCredits: 100,  // higher credits for streaming workloads
      listener: (msg) => { console.log(`Stream received: ${msg.body}`); },
    });
    _listening = true;
  }

  client.publish({
    exchange: EXCHANGE,
    routingKey: 'stream',
    body: JSON.stringify({ event: 'k6-test', iter: __ITER, vu: __VU }),
    contentType: 'application/json',
    persistent: true,
  });

  sleep(0.05);
}

export function teardown() {
  if (bindingPath) {
    client.unbindQueue(bindingPath);
  }
  // Do NOT purgeQueue on a stream — deletion is the correct cleanup path.
  client.deleteQueue(QUEUE);
  client.deleteExchange(EXCHANGE);
  client.close();
}

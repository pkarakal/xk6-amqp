// Example: manual acknowledgement with autoAck: false (AMQP 0.9.1 and 1.0).
//
// When autoAck is false the broker holds each message until the consumer
// explicitly calls msg.accept() (acknowledge) or msg.discard(requeue) (reject).
// This enables at-least-once delivery: if a VU crashes before calling accept(),
// the broker requeues the message (autoAck: false, d.Nack with requeue: true).
//
// AMQP 0.9.1: msg.discard(true) requeues the message; msg.discard(false) drops it.
// AMQP 1.0:   requeue is not supported — msg.discard() always discards the message.
//
// Run with:
//   k6 run examples/manual-ack.js
import { Client as Client091 } from 'k6/x/amqp091';
import { Client as Client10  } from 'k6/x/amqp10';
import { sleep } from 'k6';

export const options = {
  vus: 1,
  iterations: 5,
};

const client091 = new Client091({
  connectionOptions: { host: 'localhost', port: 5672, username: 'guest', password: 'guest' },
});
const client10 = new Client10({
  connectionOptions: { host: 'localhost', port: 5672, username: 'guest', password: 'guest' },
});

const QUEUE_091 = 'k6-manual-ack-091';
const QUEUE_10  = 'k6-manual-ack-10';

export function setup() {
  client091.declareQueue({ name: QUEUE_091, durable: false });
  client10.declareQueue({ name: QUEUE_10, queueType: 'classic' });
}

let _listening = false;

export default function () {
  if (!_listening) {
    // ── AMQP 0.9.1 manual ack ────────────────────────────────────────────────
    client091.listen({
      queueName: QUEUE_091,
      autoAck: false,        // broker waits for explicit accept/discard
      listener: (msg) => {
        try {
          const parsed = JSON.parse(msg.body);
          console.log(`[091] processing: iter=${parsed.iter}`);

          // Simulate a processing failure on iter 2.
          if (parsed.iter === 2) {
            // Reject and requeue so another consumer can retry.
            msg.discard(true);
            console.log('[091] rejected and requeued iter 2');
            return;
          }

          msg.accept();
          console.log(`[091] accepted: iter=${parsed.iter}`);
        } catch (e) {
          // Reject without requeue on unrecoverable errors.
          msg.discard(false);
        }
      },
    });

    // ── AMQP 1.0 manual ack ──────────────────────────────────────────────────
    client10.listen({
      queueName: QUEUE_10,
      autoAck: false,        // broker waits for explicit accept/discard
      listener: (msg) => {
        try {
          const parsed = JSON.parse(msg.body);
          console.log(`[10] processing: iter=${parsed.iter}, contentType=${msg.contentType}`);

          // Successful processing — accept the message.
          msg.accept();
          console.log(`[10] accepted: iter=${parsed.iter}`);
        } catch (e) {
          // Discard on parse failure (requeue parameter is ignored for AMQP 1.0).
          msg.discard(false);
        }
      },
    });

    _listening = true;
  }

  client091.publish({
    queueName: QUEUE_091,
    body: JSON.stringify({ iter: __ITER }),
    contentType: 'application/json',
  });

  client10.publish({
    queueName: QUEUE_10,
    body: JSON.stringify({ iter: __ITER }),
    contentType: 'application/json',
  });

  // Give the event loop time to dispatch listener callbacks.
  sleep(0.1);
}

export function teardown() {
  client091.deleteQueue(QUEUE_091);
  client091.close();
  client10.deleteQueue(QUEUE_10);
  client10.close();
}

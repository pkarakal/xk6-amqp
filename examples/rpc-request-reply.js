// Example: request-reply (RPC) pattern using correlationId and replyTo (AMQP 0.9.1).
//
// In a real RPC scenario a server process consumes requests from a shared queue,
// reads the AMQP replyTo and correlationId properties from each message, then
// publishes a response back to the replyTo queue with the same correlationId.
//
// This example is self-contained: each VU acts as both the RPC client AND the
// "server" by listening to the request queue and echoing responses back.
// The loopback is possible because we capture the replyTo queue name in a
// closure — a real server would read it from the AMQP message properties
// (the current listener callback receives only the message body string, not
// the full AMQP envelope).
//
// Per-VU exclusive reply queues:
//   - declared with exclusive: true so only this VU's connection can access them
//   - declared with deleteWhenUnused: true so the broker auto-deletes them when
//     the consumer disconnects (teardown)
//
// Run with:
//   k6 run examples/rpc-request-reply.js
import { Client } from 'k6/x/amqp091';
import exec from 'k6/execution';
import { sleep } from 'k6';

export const options = {
  vus: 3,
  duration: '20s',
};

const client = new Client({
  connectionOptions: { host: 'localhost', port: 5672, username: 'guest', password: 'guest' },
});

const REQUEST_QUEUE = 'k6-rpc-requests';

export function setup() {
  client.declareQueue({ name: REQUEST_QUEUE, durable: false });
}

let _listening = false;

export default function () {
  const vuId      = exec.vu.idInInstance;
  const replyQueue = `k6-rpc-reply-${vuId}`;

  if (!_listening) {
    // Declare an exclusive per-VU reply queue.
    // exclusive: true  → only this VU's connection can consume from it
    // deleteWhenUnused → broker auto-deletes it when the connection closes
    client.declareQueue({ name: replyQueue, exclusive: true, deleteWhenUnused: true });

    // Listen for replies addressed to this VU.
    client.listen({
      queueName: replyQueue,
      autoAck: true,
      listener: (msg) => { console.log(`VU ${vuId} received reply: ${msg}`); },
    });

    // Simulate the RPC server: consume requests and echo responses.
    // In production the server is a separate process that reads the replyTo
    // and correlationId from the AMQP message properties — not from the body.
    // Here we use a closure over replyQueue to route the echo back correctly.
    client.listen({
      queueName: REQUEST_QUEUE,
      autoAck: true,
      listener: (msg) => {
        client.publish({
          queueName: replyQueue,
          body: `Echo: ${msg}`,
          contentType: 'text/plain',
          correlationId: `corr-${vuId}`,
        });
      },
    });

    _listening = true;
  }

  // Publish the RPC request with metadata the server should echo back.
  client.publish({
    queueName: REQUEST_QUEUE,
    body: JSON.stringify({ question: 'What is 2+2?', iter: __ITER }),
    contentType: 'application/json',
    correlationId: `corr-${vuId}-${__ITER}`,
    replyTo: replyQueue,
    messageId: `${__VU}-${__ITER}`,
  });

  // Give the event loop time to dispatch both the server callback
  // (which publishes the reply) and the reply callback.
  sleep(0.2);
}

export function teardown() {
  client.deleteQueue(REQUEST_QUEUE);
  // Per-VU reply queues are exclusive and auto-deleted when their connection
  // closes — explicit deleteQueue is best-effort here.
  client.close();
}

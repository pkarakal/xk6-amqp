// Example: basic hello-world publish + consume using AMQP 0.9.1.
// This is the simplest possible xk6-amqp script — a good starting point.
//
// Run with:
//   k6 run examples/test.js
import { Client } from 'k6/x/amqp091';
import { sleep } from 'k6';

export const options = {
  vus: 2,
  iterations: 4,
};

// Clients are created in the init context (no IO yet) and connected lazily.
// Each VU that constructs a Client gets its own isolated connection.
const client = new Client({
  connectionOptions: { host: 'localhost', port: 5672, username: 'guest', password: 'guest' },
});

export function setup() {
  // Declare the queue once before VUs start.
  client.declareQueue({ name: 'k6-general', durable: false });
}

// Per-VU flag: each VU starts its consumer once on the first iteration.
let _listening = false;

export default function () {
  // listen() must be called from default(), not setup(), because the listener
  // callback is dispatched on the VU event loop which only runs here.
  if (!_listening) {
    client.listen({
      queueName: 'k6-general',
      autoAck: true,
      listener: (msg) => { console.log('Received:', msg); },
    });
    _listening = true;
  }

  client.publish({
    queueName: 'k6-general',
    body: 'Ping from k6',
    contentType: 'text/plain',
  });

  // sleep() gives the event loop time to dispatch queued listener callbacks.
  sleep(0.1);
}

export function teardown() {
  client.deleteQueue('k6-general');
  client.close();
}

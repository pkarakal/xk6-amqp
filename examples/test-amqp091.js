/**
 * Integration test for the AMQP 0.9.1 module (k6/x/amqp091).
 *
 * Verifies: connect, declareExchange, declareQueue, bindQueue,
 *           publish, listen, close.
 *
 * Note: listen() must be called from default() (not setup()), because the
 * listener callback is dispatched via the VU's event loop, which only runs
 * during default() iterations (e.g., during sleep()). The setup() VU's event
 * loop is idle during the main test phase.
 */
import { Client } from 'k6/x/amqp091';
import { sleep } from 'k6';

export const options = {
  vus: 2,
  iterations: 4,
};

const EXCHANGE = 'k6-test-091';
const QUEUE    = 'k6-test-queue-091';
const ROUTING  = 'test';

const client = new Client({
  connectionOptions: { host: 'localhost', port: 5672, username: 'guest', password: 'guest' },
});

// Per-VU flag: each VU calls listen() exactly once on its first iteration.
let _listening = false;

export function setup() {
  // Declare resources once before VUs start.
  client.declareExchange({ name: EXCHANGE, kind: 'direct', durable: false });
  client.declareQueue({ name: QUEUE, durable: false });
  client.bindQueue({ queueName: QUEUE, exchangeName: EXCHANGE, routingKey: ROUTING });
}

export default function () {
  // Start the consumer on the first iteration of each VU.
  // The callback fires on this VU's event loop (during sleep below).
  if (!_listening) {
    client.listen({
      queueName: QUEUE,
      autoAck: true,
      listener: (msg) => { console.log(`Received: ${msg.body}`); },
    });
    _listening = true;
  }

  const body = `hello from VU ${__VU} iter ${__ITER}`;

  client.publish({
    exchange: EXCHANGE,
    routingKey: ROUTING,
    contentType: 'text/plain',
    body: body,
  });

  // The event loop dispatches pending listener callbacks during sleep.
  sleep(0.1);
}

export function teardown() {
  client.purgeQueue(QUEUE);
  client.close();
  console.log('AMQP 0.9.1 test complete');
}

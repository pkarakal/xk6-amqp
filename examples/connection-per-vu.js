// Example: one AMQP connection per VU.
//
// In the new Client API, declaring `const client = new Client(...)` at module
// scope is sufficient — k6 re-evaluates module-scope code for each VU, so every
// VU automatically gets its own isolated connection. No manual connection-ID
// tracking (as in the legacy Amqp.start() API) is required.
//
// Run with:
//   k6 run examples/connection-per-vu.js
import { Client } from 'k6/x/amqp091';
import exec from 'k6/execution';

export const options = {
  vus: 5,
  duration: '10s',
};

// One Client (and therefore one connection) is created per VU automatically.
const client = new Client({
  connectionOptions: { host: 'localhost', port: 5672, username: 'guest', password: 'guest' },
});

export function setup() {
  client.declareQueue({ name: 'k6-per-vu-queue', durable: false });
}

export default function () {
  const vuId = exec.vu.idInInstance;
  console.log(`VU ${vuId} publishing on its own connection`);

  client.publish({
    queueName: 'k6-per-vu-queue',
    contentType: 'text/plain',
    body: `Message from VU ${vuId}`,
  });
}

export function teardown() {
  client.deleteQueue('k6-per-vu-queue');
  client.close();
}

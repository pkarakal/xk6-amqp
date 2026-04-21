// Example: delete an exchange (AMQP 0.9.1).
// The script is self-contained: it declares the exchange first so it can be run
// against any broker without pre-existing resources.
//
// Run with:
//   k6 run examples/delete-exchange.js
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
  client.deleteExchange('k6-exchange');
  console.log('k6-exchange deleted');
}

export default function () {
  sleep(0);
}

export function teardown() {
  client.close();
}

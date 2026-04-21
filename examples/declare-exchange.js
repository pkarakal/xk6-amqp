// Example: declare an exchange with all available options (AMQP 0.9.1).
// Use this as a reference card for declareExchange parameters.
//
// Valid kind values: 'direct' | 'topic' | 'fanout' | 'headers'
// The 'internal' flag is AMQP 0.9.1-specific (not available in AMQP 1.0).
//
// Run with:
//   k6 run examples/declare-exchange.js
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
  client.declareExchange({
    name: 'k6-exchange',
    kind: 'direct',      // 'direct' | 'topic' | 'fanout' | 'headers'
    durable: false,      // do not survive broker restarts
    autoDelete: false,   // do not delete when last binding is removed
    internal: false,     // not an internal exchange (0.9.1-only flag)
    noWait: false,       // wait for broker confirmation
    // args: null,
  });

  console.log('k6-exchange declared');
}

export default function () {
  sleep(0);
}

export function teardown() {
  client.deleteExchange('k6-exchange');
  client.close();
}

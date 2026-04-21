// Example: bind a destination exchange to a source exchange (AMQP 0.9.1).
// Exchange-to-exchange bindings allow routing messages through chains of exchanges.
//
// AMQP 0.9.1 bindExchange parameter names:
//   { source, destination, routingKey?, noWait?, args? }
//
// AMQP 1.0 bindExchange parameter names (different!):
//   { sourceExchange, destinationExchange, bindingKey?, args? }
//   — and bindExchange returns a binding path string required by unbindExchange.
//
// Run with:
//   k6 run examples/bind-exchange.js
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
  client.declareExchange({ name: 'k6-source-exchange', kind: 'topic', durable: false });
  client.declareExchange({ name: 'k6-dest-exchange', kind: 'direct', durable: false });

  // Bind: messages published to k6-source-exchange with key matching 'k6.*'
  // are forwarded to k6-dest-exchange.
  client.bindExchange({
    source: 'k6-source-exchange',
    destination: 'k6-dest-exchange',
    routingKey: 'k6.*',
    noWait: false,
    // args: null,
  });

  console.log('k6-dest-exchange bound to k6-source-exchange');
}

export default function () {
  sleep(0);
}

export function teardown() {
  client.deleteExchange('k6-dest-exchange');
  client.deleteExchange('k6-source-exchange');
  client.close();
}

// Example: remove an exchange-to-exchange binding (AMQP 0.9.1).
//
// IMPORTANT: For AMQP 0.9.1, use unbindExchangeWithOptions({ source, destination, routingKey }).
// client.unbindExchange(path) always throws for 0.9.1 because the protocol has no binding path concept.
//
// For AMQP 1.0, use unbindExchange(bindingPath) where bindingPath is the string
// returned by bindExchange({ sourceExchange, destinationExchange, bindingKey }).
//
// Run with:
//   k6 run examples/unbind-exchange.js
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
  client.declareExchange({ name: 'k6-source', kind: 'topic', durable: false });
  client.declareExchange({ name: 'k6-dest', kind: 'direct', durable: false });
  client.bindExchange({ source: 'k6-source', destination: 'k6-dest', routingKey: 'k6.*' });

  // AMQP 0.9.1: use unbindExchangeWithOptions (must match the binding exactly).
  client.unbindExchangeWithOptions({
    source: 'k6-source',
    destination: 'k6-dest',
    routingKey: 'k6.*',
    // args: null,
  });

  console.log('k6-dest unbound from k6-source');
}

export default function () {
  sleep(0);
}

export function teardown() {
  client.deleteExchange('k6-dest');
  client.deleteExchange('k6-source');
  client.close();
}

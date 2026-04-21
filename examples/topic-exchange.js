// Example: topic exchange with wildcard routing keys (AMQP 0.9.1).
//
// Topic exchanges route messages based on dot-separated routing keys using two
// wildcard characters:
//   *  matches exactly one word  (e.g. 'orders.*' matches 'orders.new' but NOT 'orders.eu.large')
//   #  matches zero or more words (e.g. 'orders.#' matches 'orders.new' AND 'orders.eu.large')
//
// This example binds three queues with different patterns and publishes three
// messages to demonstrate selective routing.
//
// Run with:
//   k6 run examples/topic-exchange.js
import { Client } from 'k6/x/amqp091';
import { sleep } from 'k6';

export const options = {
  vus: 1,
  iterations: 3,
};

const client = new Client({
  connectionOptions: { host: 'localhost', port: 5672, username: 'guest', password: 'guest' },
});

const EXCHANGE = 'k6-topic';
const QUEUES = {
  ordersAny:  'k6-orders-any',   // pattern: 'orders.*'
  ordersDeep: 'k6-orders-deep',  // pattern: 'orders.#'
  critical:   'k6-critical',     // pattern: '*.critical'
};

export function setup() {
  client.declareExchange({ name: EXCHANGE, kind: 'topic', durable: false });

  client.declareQueue({ name: QUEUES.ordersAny, durable: false });
  client.bindQueue({ queueName: QUEUES.ordersAny, exchangeName: EXCHANGE, routingKey: 'orders.*' });

  client.declareQueue({ name: QUEUES.ordersDeep, durable: false });
  client.bindQueue({ queueName: QUEUES.ordersDeep, exchangeName: EXCHANGE, routingKey: 'orders.#' });

  client.declareQueue({ name: QUEUES.critical, durable: false });
  client.bindQueue({ queueName: QUEUES.critical, exchangeName: EXCHANGE, routingKey: '*.critical' });
}

let _listening = false;

export default function () {
  if (!_listening) {
    const makeListener = (queueName) => (msg) => console.log(`[${queueName}] received: ${msg}`);
    client.listen({ queueName: QUEUES.ordersAny,  autoAck: true, listener: makeListener(QUEUES.ordersAny) });
    client.listen({ queueName: QUEUES.ordersDeep, autoAck: true, listener: makeListener(QUEUES.ordersDeep) });
    client.listen({ queueName: QUEUES.critical,   autoAck: true, listener: makeListener(QUEUES.critical) });
    _listening = true;
  }

  // → routed to: orders-any + orders-deep  (single word after 'orders.')
  client.publish({ exchange: EXCHANGE, routingKey: 'orders.new',       body: 'new order',       contentType: 'text/plain' });
  // → routed to: orders-deep only          (multi-word, '*' can't match 'eu.large')
  client.publish({ exchange: EXCHANGE, routingKey: 'orders.eu.large',  body: 'large EU order',  contentType: 'text/plain' });
  // → routed to: critical only             ('*' matches 'payments', not 'orders.*' or 'orders.#')
  client.publish({ exchange: EXCHANGE, routingKey: 'payments.critical', body: 'payment failure', contentType: 'text/plain' });

  sleep(0.2);
}

export function teardown() {
  Object.values(QUEUES).forEach((name) => {
    client.purgeQueue(name);
    client.deleteQueue(name);
  });
  client.deleteExchange(EXCHANGE);
  client.close();
}

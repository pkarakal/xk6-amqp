// Example: fanout exchange broadcasting to multiple queues (AMQP 0.9.1).
//
// A fanout exchange ignores the routing key entirely and delivers every message
// to all bound queues. This is the simplest broadcast pattern — one publish
// reaches N consumers simultaneously.
//
// Run with:
//   k6 run examples/fanout-exchange.js
import { Client } from 'k6/x/amqp091';
import { sleep } from 'k6';

export const options = {
  vus: 1,
  iterations: 3,
};

const client = new Client({
  connectionOptions: { host: 'localhost', port: 5672, username: 'guest', password: 'guest' },
});

const EXCHANGE = 'k6-fanout';
const QUEUES   = ['k6-fan-q1', 'k6-fan-q2', 'k6-fan-q3'];

export function setup() {
  client.declareExchange({ name: EXCHANGE, kind: 'fanout', durable: false });

  QUEUES.forEach((name) => {
    client.declareQueue({ name, durable: false });
    // routingKey is ignored by fanout exchanges; '' is conventional.
    client.bindQueue({ queueName: name, exchangeName: EXCHANGE, routingKey: '' });
  });
}

let _listening = false;

export default function () {
  if (!_listening) {
    QUEUES.forEach((name) => {
      client.listen({
        queueName: name,
        autoAck: true,
        listener: (msg) => { console.log(`[${name}] broadcast received: ${msg}`); },
      });
    });
    _listening = true;
  }

  // A single publish is delivered to all three bound queues.
  client.publish({
    exchange: EXCHANGE,
    routingKey: '',   // ignored by fanout exchanges
    body: `Broadcast from iter ${__ITER}`,
    contentType: 'text/plain',
  });

  sleep(0.2);
}

export function teardown() {
  QUEUES.forEach((name) => {
    client.purgeQueue(name);
    client.deleteQueue(name);
  });
  client.deleteExchange(EXCHANGE);
  client.close();
}

// Example: MessagePack serialization with AMQP 0.9.1.
//
// When contentType is 'application/x-msgpack', the Go extension JSON-parses
// the body string you pass and re-encodes it as MessagePack bytes before
// publishing. The listener receives the raw msgpack bytes as a string.
//
// Run with:
//   k6 run examples/test-msgpack.js
import { Client } from 'k6/x/amqp091';
import { sleep } from 'k6';

export const options = {
  vus: 1,
  iterations: 3,
};

const client = new Client({
  connectionOptions: { host: 'localhost', port: 5672, username: 'guest', password: 'guest' },
});

export function setup() {
  client.declareQueue({ name: 'k6-msgpack', durable: false });
}

let _listening = false;

export default function () {
  if (!_listening) {
    client.listen({
      queueName: 'k6-msgpack',
      autoAck: true,
      // The listener receives the raw msgpack-encoded bytes as a string.
      listener: (msg) => { console.log('Received msgpack payload (raw bytes):', msg); },
    });
    _listening = true;
  }

  // Pass a JSON string as body; the extension will re-encode it as msgpack.
  const body = JSON.stringify({
    metadata: { header1: 'Performance Test Message' },
    payload:  { field1: 'some value', iter: __ITER },
  });

  client.publish({
    queueName: 'k6-msgpack',
    body: body,
    contentType: 'application/x-msgpack',
  });

  sleep(0.1);
}

export function teardown() {
  client.deleteQueue('k6-msgpack');
  client.close();
}

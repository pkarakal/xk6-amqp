# xk6-amqp

A [k6](https://k6.io) extension for publishing and consuming messages over AMQP.
Supports both **AMQP 0.9.1** (RabbitMQ classic) and **AMQP 1.0** (RabbitMQ with the AMQP 1.0 plugin, Azure Service Bus, ActiveMQ, etc.).

| Import path | Protocol | Notes |
|---|---|---|
| `k6/x/amqp091` | AMQP 0.9.1 | Full feature set |
| `k6/x/amqp` | AMQP 0.9.1 | Alias for `k6/x/amqp091` |
| `k6/x/amqp10` | AMQP 1.0 | Quorum and Stream queues supported |

---

## Build

Prerequisites: [Go toolchain](https://go101.org/article/go-toolchain.html) and Git.

```bash
# Install xk6
go install go.k6.io/xk6/cmd/xk6@latest

# Build k6 with the extension
xk6 build --with github.com/pkarakal/xk6-amqp@latest
```

### Development build

Use the `Makefile` to format, test, and build from local source:

```bash
git clone git@github.com:pkarakal/xk6-amqp.git
cd xk6-amqp
make        # format + unit tests + build
```

Available targets:

| Target | Description |
|---|---|
| `make build` | Build `./k6` binary with the local extension |
| `make test` | Unit tests only (no broker required) |
| `make test-integration` | All tests including integration (requires Docker) |
| `make format` | Apply `go fmt` |
| `make clean` | Remove the built `./k6` binary |

---

## Quick start

### AMQP 0.9.1

```javascript
import { Client } from 'k6/x/amqp091';
import { sleep } from 'k6';

export const options = { vus: 2, iterations: 4 };

// Clients are constructed in the init context (no IO yet).
// Each VU gets its own isolated connection automatically.
const client = new Client({
  connectionOptions: { host: 'localhost', port: 5672, username: 'guest', password: 'guest' },
});

export function setup() {
  client.declareExchange({ name: 'k6-exchange', kind: 'topic', durable: true });
  client.declareQueue({ name: 'k6-queue', durable: true });
  client.bindQueue({ queueName: 'k6-queue', exchangeName: 'k6-exchange', routingKey: 'k6' });
}

// Per-VU flag: start the consumer once on the first iteration.
let _listening = false;

export default function () {
  // listen() must be called from default(), not setup() — the listener
  // callback is dispatched on the VU event loop, which only runs here.
  if (!_listening) {
    client.listen({
      queueName: 'k6-queue',
      autoAck: true,
      listener: (msg) => { console.log('Received:', msg.body); },
    });
    _listening = true;
  }

  client.publish({
    exchange: 'k6-exchange',
    routingKey: 'k6',
    body: 'Ping from k6',
    contentType: 'text/plain',
  });

  sleep(0.1); // gives the event loop time to dispatch listener callbacks
}

export function teardown() {
  client.deleteQueue('k6-queue');
  client.deleteExchange('k6-exchange');
  client.close();
}
```

### AMQP 1.0

> **Network limitation:** AMQP 1.0 connections bypass k6's `netext.Dialer`. As a result, k6's DNS override, proxy settings, and network-level metrics (e.g. `http_req_connecting`) do **not** apply to AMQP 1.0 traffic. Use AMQP 0.9.1 (`k6/x/amqp091`) if you require k6 network instrumentation.

```javascript
import { Client } from 'k6/x/amqp10';
import { sleep } from 'k6';

export const options = { vus: 2, iterations: 4 };

const client = new Client({
  connectionOptions: { host: 'localhost', port: 5672, username: 'guest', password: 'guest' },
});

// AMQP 1.0: bindQueue returns a binding path required by unbindQueue.
let bindingPath;

export function setup() {
  client.declareExchange({ name: 'k6-exchange', kind: 'direct' });
  client.declareQueue({ name: 'k6-queue', queueType: 'classic' });
  bindingPath = client.bindQueue({
    sourceExchange: 'k6-exchange',
    destinationQueue: 'k6-queue',
    bindingKey: 'k6',
  });
}

let _listening = false;

export default function () {
  if (!_listening) {
    client.listen({
      queueName: 'k6-queue',
      autoAck: true,
      initialCredits: 256,  // AMQP 1.0 link flow control
      listener: (msg) => { console.log('Received:', msg.body); },
    });
    _listening = true;
  }

  client.publish({
    exchange: 'k6-exchange',
    routingKey: 'k6',
    body: 'Ping from k6 via AMQP 1.0',
    contentType: 'text/plain',
  });

  sleep(0.1);
}

export function teardown() {
  if (bindingPath) client.unbindQueue(bindingPath);
  client.deleteQueue('k6-queue');
  client.deleteExchange('k6-exchange');
  client.close();
}
```

---

## API reference

### `new Client(opts)`

Creates a client. The connection is established lazily on first I/O.
Because k6 evaluates module-scope code per VU, each `const client = new Client(...)` at module scope automatically results in one connection per VU.

```typescript
interface ClientOptions {
  connectionOptions: {
    host:     string;
    port:     number;
    username: string;
    password: string;
  };
  options?: Record<string, unknown>; // protocol-level options (e.g. TLS config)
}
```

---

### Publishing

#### `client.publish(options)` — AMQP 0.9.1 and 1.0

Publish a message synchronously.

**AMQP 0.9.1 options:**

| Field | Type | Description |
|---|---|---|
| `queueName` | `string` | Publish directly to a queue. Mutually exclusive with `exchange`. |
| `exchange` | `string` | Publish to an exchange. Mutually exclusive with `queueName`. |
| `routingKey` | `string` | Routing key used when publishing to an exchange. |
| `body` | `string \| ArrayBuffer` | Message body. |
| `contentType` | `string` | MIME type (e.g. `"text/plain"`, `"application/json"`, `"application/x-msgpack"`). |
| `headers` | `Record<string, unknown>` | Custom message headers. |
| `persistent` | `boolean` | Mark as persistent (delivery mode 2). |
| `mandatory` | `boolean` | Return if not routable. |
| `immediate` | `boolean` | Return if no consumer ready. |
| `correlationId` | `string` | RPC correlation ID. |
| `replyTo` | `string` | Reply-to queue name (RPC pattern). |
| `expiration` | `string` | Message TTL in ms as a string (e.g. `"60000"`). |
| `messageId` | `string` | Application message ID. |
| `timestamp` | `number` | Unix epoch seconds. |
| `type` | `string` | Message type label. |
| `userId` | `string` | Publishing user. |
| `appId` | `string` | Publishing application. |

**AMQP 1.0 options** (subset — no `headers`, `mandatory`, `immediate`, `replyTo`, `expiration`, `timestamp`, `type`, `userId`, `appId`):

| Field | Type | Description |
|---|---|---|
| `queueName` | `string` | Publish directly to a queue. |
| `exchange` | `string` | Publish to an exchange. |
| `routingKey` | `string` | Routing key. |
| `body` | `string \| ArrayBuffer` | Message body. |
| `contentType` | `string` | MIME type. Maps to AMQP 1.0 `Properties.ContentType`. |
| `persistent` | `boolean` | Set the Durable flag on the AMQP 1.0 message header. |
| `subject` | `string` | Maps to AMQP 1.0 `Properties.Subject`. |
| `correlationId` | `string` | Maps to AMQP 1.0 `Properties.CorrelationID`. |
| `messageId` | `string` | Maps to AMQP 1.0 `Properties.MessageID`. |

#### `client.publishAsync(options)` — AMQP 0.9.1 only

Identical options to `publish`. Returns `Promise<void>` that resolves when the broker **confirms delivery** via publisher confirms (`ch.Confirm`). The promise is rejected if the broker nacks the message or the VU context is cancelled. Use with `async default()` and `await`:

```javascript
export default async function () {
  await client.publishAsync({
    exchange: 'k6-exchange',
    routingKey: 'k6',
    body: 'Confirmed message',
    persistent: true,
    contentType: 'text/plain',
  });
}
```

> **Note:** Publisher confirms operate at the broker level — they confirm that the message was received and enqueued, not that it was consumed or persisted to disk. For strict at-least-once delivery, combine with durable queues and `persistent: true`.

---

### Consuming

#### `client.listen(options)`

Start consuming messages from a queue. The `listener` callback is invoked on the VU event loop — safe for recording k6 metrics, using `check()`, etc.

**Important:** `listen()` must be called from `default()`, not `setup()`. The VU event loop (which dispatches listener callbacks) only runs during default iterations. Use a per-VU guard flag to call it only once per VU.

**AMQP 0.9.1 options:**

| Field | Type | Default | Description |
|---|---|---|---|
| `queueName` | `string` | — | Queue to consume from. |
| `listener` | `(msg: Message) => void` | — | Called for each received message. |
| `autoAck` | `boolean` | `false` | Automatically acknowledge on delivery. When `false`, call `msg.accept()` or `msg.discard()`. |
| `consumer` | `string` | `""` | Consumer tag. |
| `exclusive` | `boolean` | `false` | Exclusive consumer access. |
| `noLocal` | `boolean` | `false` | Do not deliver messages published by this connection. |
| `noWait` | `boolean` | `false` | Do not wait for broker confirmation. |
| `args` | `Record<string, unknown>` | `null` | Additional arguments. |

**AMQP 1.0 options:**

| Field | Type | Default | Description |
|---|---|---|---|
| `queueName` | `string` | — | Queue to consume from. |
| `listener` | `(msg: Message) => void` | — | Called for each received message. |
| `autoAck` | `boolean` | `false` | Automatically accept (ack) messages after the listener returns without error. When `false`, call `msg.accept()` or `msg.discard()`. |
| `consumer` | `string` | `""` | Receiver link name. |
| `initialCredits` | `number` | `256` | AMQP 1.0 link flow control credits (prefetch). |

#### Message object

Each listener callback receives a `Message` object with the following fields:

| Field | Type | Description |
|---|---|---|
| `body` | `string` | Message payload decoded as UTF-8. |
| `routingKey` | `string` | Routing key (0.9.1) or `Properties.Subject` (1.0). |
| `contentType` | `string` | MIME content type. |
| `correlationId` | `string` | Correlation ID (useful in RPC patterns). |
| `messageId` | `string` | Message identifier. |
| `headers` | `Record<string, unknown>` | Application headers (0.9.1) or application properties (1.0). |
| `accept()` | `() => void` | Acknowledge the message. Only meaningful when `autoAck: false`. |
| `discard(requeue?)` | `(boolean) => void` | Reject the message. Pass `true` to requeue (0.9.1 only; ignored for AMQP 1.0). Only meaningful when `autoAck: false`. |

**Manual acknowledgement example:**

```javascript
client.listen({
  queueName: 'k6-queue',
  autoAck: false,  // hold messages until accept() or discard() is called
  listener: (msg) => {
    try {
      const data = JSON.parse(msg.body);
      console.log('Processing:', data, 'correlationId:', msg.correlationId);
      msg.accept();                // broker removes the message
    } catch (e) {
      msg.discard(true);           // broker requeues for retry (AMQP 0.9.1 only)
    }
  },
});
```

See [`examples/manual-ack.js`](./examples/manual-ack.js) for a complete working example with both protocols.

---

### Exchange management

#### `client.declareExchange(options)`

| Field | Type | Notes |
|---|---|---|
| `name` | `string` | |
| `kind` | `'direct' \| 'topic' \| 'fanout' \| 'headers'` | |
| `durable` | `boolean` | 0.9.1 only |
| `autoDelete` | `boolean` | |
| `internal` | `boolean` | 0.9.1 only |
| `noWait` | `boolean` | 0.9.1 only |
| `args` | `Record<string, unknown>` | |

#### `client.deleteExchange(name: string)`

#### `client.bindExchange(options)` → `string`

Binds a destination exchange to a source exchange.

| Protocol | Parameters |
|---|---|
| AMQP 0.9.1 | `{ source, destination, routingKey?, noWait?, args? }` |
| AMQP 1.0 | `{ sourceExchange, destinationExchange, bindingKey?, args? }` |

Returns: `""` for AMQP 0.9.1, a binding path string for AMQP 1.0.

> **AMQP 1.0 field aliases:** The 0.9.1-style names `source`, `destination`, and `routingKey` are accepted as aliases for `sourceExchange`, `destinationExchange`, and `bindingKey`. The native AMQP 1.0 names take precedence when both are present.

#### `client.unbindExchangeWithOptions(options)` — AMQP 0.9.1

Remove an exchange binding. Parameters: `{ source, destination, routingKey?, args? }`.

> Use this instead of `unbindExchange(path)` for AMQP 0.9.1 — `unbindExchange` always throws for this protocol.

#### `client.unbindExchange(bindingPath: string)` — AMQP 1.0

Remove an exchange binding using the path returned by `bindExchange`.

---

### Queue management

#### `client.declareQueue(options)`

| Field | Type | Notes |
|---|---|---|
| `name` | `string` | |
| `durable` | `boolean` | 0.9.1 only |
| `deleteWhenUnused` | `boolean` | 0.9.1 only (was `delete_when_unused` in legacy API) |
| `exclusive` | `boolean` | |
| `noWait` | `boolean` | 0.9.1 only |
| `queueType` | `'classic' \| 'quorum' \| 'stream'` | 1.0 only |
| `autoDelete` | `boolean` | 1.0 only |
| `args` | `Record<string, unknown>` | |

#### `client.deleteQueue(name: string)`

#### `client.bindQueue(options)` → `string`

Binds a queue to an exchange.

| Protocol | Parameters |
|---|---|
| AMQP 0.9.1 | `{ queueName, exchangeName, routingKey?, noWait?, args? }` |
| AMQP 1.0 | `{ sourceExchange, destinationQueue, bindingKey?, args? }` |

Returns: `""` for AMQP 0.9.1, a binding path string for AMQP 1.0.

> **AMQP 1.0 field aliases:** The 0.9.1-style names `exchangeName`, `queueName`, and `routingKey` are accepted as aliases for `sourceExchange`, `destinationQueue`, and `bindingKey`. The native AMQP 1.0 names take precedence when both are present.

#### `client.unbindQueueWithOptions(options)` — AMQP 0.9.1

Remove a queue binding. Parameters: `{ queueName, exchangeName, routingKey?, args? }`.

> Use this instead of `unbindQueue(path)` for AMQP 0.9.1 — `unbindQueue` always throws for this protocol.

#### `client.unbindQueue(bindingPath: string)` — AMQP 1.0

Remove a queue binding using the path returned by `bindQueue`.

#### `client.purgeQueue(name: string)` → `number`

Remove all messages from a queue. Returns the number of messages purged.

> **Note:** Do not call `purgeQueue` on stream queues — stream queues truncate by offset, not by consumer acknowledgement.

#### `client.inspectQueue(name: string)` → `{ name, messages, consumers }`

Return queue metadata without modifying it. Requires the queue to already exist.

---

### Lifecycle

#### `client.close()`

Stop all consumer goroutines and close the AMQP connection.

---

## MessagePack

When `contentType` is set to `'application/x-msgpack'`, the extension JSON-parses the body string you provide and re-encodes it as MessagePack bytes before publishing. Pass a `JSON.stringify()`-encoded string as the body:

```javascript
client.publish({
  queueName: 'k6-msgpack',
  body: JSON.stringify({ event: 'test', value: 42 }),
  contentType: 'application/x-msgpack',
});
```

---

## Protocol comparison

| Feature | AMQP 0.9.1 (`k6/x/amqp091`) | AMQP 1.0 (`k6/x/amqp10`) |
|---|---|---|
| Async publish | `publishAsync()` → `Promise<void>` | Not available |
| Queue types | Classic only | `classic`, `quorum`, `stream` |
| Flow control | — | `initialCredits` on listen |
| Exchange bind params | `source` / `destination` / `routingKey` | `sourceExchange` / `destinationExchange` / `bindingKey` |
| Queue bind params | `queueName` / `exchangeName` / `routingKey` | `sourceExchange` / `destinationQueue` / `bindingKey` |
| Unbind | `unbindQueueWithOptions` / `unbindExchangeWithOptions` | `unbindQueue(path)` / `unbindExchange(path)` |
| Message metadata | `headers`, `mandatory`, `immediate`, `replyTo`, `expiration`, `timestamp`, `type`, `userId`, `appId` | `subject` |
| Binding path | Not returned (returns `""`) | Returned by `bindQueue` / `bindExchange` |
| MessagePack | Supported | Not supported |

---

## TypeScript types

Full TypeScript declarations for all three import paths are available in [`index.d.ts`](./index.d.ts).

---

## Examples

The [`examples/`](./examples/) directory contains runnable scripts for every supported pattern:

### Core pub/sub
| File | Description |
|---|---|
| [`publish-listen.js`](./examples/publish-listen.js) | Topic exchange pub/sub, AMQP 0.9.1, load test with stages |
| [`publish-listen-amqp10.js`](./examples/publish-listen-amqp10.js) | Same pattern using AMQP 1.0 |
| [`test-amqp091.js`](./examples/test-amqp091.js) | Integration test for AMQP 0.9.1 |
| [`test-amqp10.js`](./examples/test-amqp10.js) | Integration test for AMQP 1.0 |
| [`test.js`](./examples/test.js) | Minimal hello-world (AMQP 0.9.1) |

### Advanced patterns
| File | Description |
|---|---|
| [`publish-async.js`](./examples/publish-async.js) | `publishAsync` + `async default()` with publisher confirms |
| [`manual-ack.js`](./examples/manual-ack.js) | Manual ack/nack with `autoAck: false`, `msg.accept()`, `msg.discard()` |
| [`topic-exchange.js`](./examples/topic-exchange.js) | Topic exchange with `*` / `#` wildcard routing keys |
| [`fanout-exchange.js`](./examples/fanout-exchange.js) | Fanout exchange broadcasting to multiple queues |
| [`rpc-request-reply.js`](./examples/rpc-request-reply.js) | RPC pattern using `msg.correlationId` + `replyTo` |
| [`connection-per-vu.js`](./examples/connection-per-vu.js) | Per-VU connection isolation |
| [`test-msgpack.js`](./examples/test-msgpack.js) | MessagePack serialization |

### AMQP 1.0 specific
| File | Description |
|---|---|
| [`quorum-queue.js`](./examples/quorum-queue.js) | Quorum queue with `initialCredits` |
| [`stream-queue.js`](./examples/stream-queue.js) | Stream queue with size-based retention |

### Management operations
| File | Description |
|---|---|
| [`declare-queue.js`](./examples/declare-queue.js) | `declareQueue` — all options |
| [`declare-exchange.js`](./examples/declare-exchange.js) | `declareExchange` — all options |
| [`delete-queue.js`](./examples/delete-queue.js) | `deleteQueue` |
| [`delete-exchange.js`](./examples/delete-exchange.js) | `deleteExchange` |
| [`bind-queue.js`](./examples/bind-queue.js) | `bindQueue` — 0.9.1 parameter names |
| [`bind-exchange.js`](./examples/bind-exchange.js) | `bindExchange` — exchange-to-exchange binding |
| [`unbind-queue.js`](./examples/unbind-queue.js) | `unbindQueueWithOptions` (0.9.1) |
| [`unbind-exchange.js`](./examples/unbind-exchange.js) | `unbindExchangeWithOptions` (0.9.1) |
| [`inspect-queue.js`](./examples/inspect-queue.js) | `inspectQueue` → `{ name, messages, consumers }` |
| [`purge-queue.js`](./examples/purge-queue.js) | `purgeQueue` — returns message count |

---

## Testing locally

A [`docker-compose.yml`](./docker-compose.yml) file starts RabbitMQ with the Management Plugin:

```bash
docker compose up -d
```

This starts:
- RabbitMQ broker on `localhost:5672`
- Management UI on [http://localhost:15672](http://localhost:15672) (login: `guest` / `guest`)

Run a test script against it:

```bash
make build
./k6 run examples/test.js
```

For AMQP 1.0 examples, enable the plugin inside the running container:

```bash
docker compose exec rabbitmq rabbitmq-plugins enable rabbitmq_amqp1_0
```

For quorum queue examples no extra steps are needed — quorum queues are built into RabbitMQ 3.8+.

For stream queue examples, enable the stream plugin:

```bash
docker compose exec rabbitmq rabbitmq-plugins enable rabbitmq_stream
```

> This environment is intended for local development only and should not be used in production.

/**
 * TypeScript type declarations for the xk6-amqp k6 extension.
 *
 * Covers three import paths:
 *   import { Client } from 'k6/x/amqp';      // alias for amqp091
 *   import { Client } from 'k6/x/amqp091';
 *   import { Client } from 'k6/x/amqp10';
 */

// ── k6/x/amqp091 ─────────────────────────────────────────────────────────────

declare module 'k6/x/amqp091' {
  /** AMQP exchange type. */
  export type ExchangeKind = 'direct' | 'topic' | 'fanout' | 'headers';

  /**
   * A received AMQP message passed to the {@link ListenOptions.listener} callback.
   *
   * `accept` and `discard` are meaningful only when `autoAck` is `false`.
   * When `autoAck` is `true` they are no-ops — the broker acknowledges
   * delivery automatically.
   */
  export interface Message {
    /** Message payload decoded as a UTF-8 string. */
    body:            string;
    /** Routing key the message was published with. */
    routingKey?:     string;
    /** MIME content type (e.g. `"application/json"`). */
    contentType?:    string;
    /** Correlation identifier set by the publisher. */
    correlationId?:  string;
    /** Application headers attached to the message. */
    headers?:        Record<string, unknown>;
    /** Message identifier set by the publisher. */
    messageId?:      string;
    /**
     * Acknowledge the message.
     * Only meaningful when `autoAck` is `false`.
     */
    accept(): void;
    /**
     * Reject the message.
     * @param requeue - If `true` the broker requeues the message.
     * Only meaningful when `autoAck` is `false`.
     */
    discard(requeue?: boolean): void;
  }

  /** Options used to establish a broker connection. */
  export interface ConnectionOptions {
    host:     string;
    port:     number;
    username: string;
    password: string;
  }

  /**
   * Queue metadata returned by {@link Client.inspectQueue}.
   * Shape matches `amqp091.Queue`.
   */
  export interface QueueInfo {
    name:      string;
    messages:  number;
    consumers: number;
  }

  /** Options for {@link Client.publish} and {@link Client.publishAsync}. */
  export interface PublishOptions {
    /** Publish directly to a queue. Mutually exclusive with `exchange`. */
    queueName?:     string;
    /** Publish to an exchange. Mutually exclusive with `queueName`. */
    exchange?:      string;
    /** Routing key used when publishing to an exchange. */
    routingKey?:    string;
    /** Message body. */
    body?:          string | ArrayBuffer;
    /** Custom message headers. */
    headers?:       Record<string, unknown>;
    /** MIME content type (e.g. `"application/json"`, `"application/x-msgpack"`). */
    contentType?:   string;
    /** If true the broker must route the message or return it. */
    mandatory?:     boolean;
    /** If true the broker must deliver immediately or return the message. */
    immediate?:     boolean;
    /** Mark the message as persistent (delivery mode 2). */
    persistent?:    boolean;
    correlationId?: string;
    replyTo?:       string;
    /** Message TTL in milliseconds as a string (e.g. `"60000"`). */
    expiration?:    string;
    messageId?:     string;
    /** Unix epoch seconds. */
    timestamp?:     number;
    type?:          string;
    userId?:        string;
    appId?:         string;
  }

  /** Options for {@link Client.listen}. */
  export interface ListenOptions {
    /** Queue to consume from. */
    queueName:  string;
    /** Consumer tag. */
    consumer?:  string;
    /** Automatically acknowledge messages on delivery. */
    autoAck?:   boolean;
    /** Request exclusive consumer access. */
    exclusive?: boolean;
    /** Do not deliver messages published by this connection. */
    noLocal?:   boolean;
    /** Do not wait for the broker to confirm. */
    noWait?:    boolean;
    /** Additional arguments passed to the broker. */
    args?:      Record<string, unknown>;
    /**
     * Called for each received message.
     * When `autoAck` is `false`, call `msg.accept()` or `msg.discard()` to
     * acknowledge or reject the message explicitly.
     */
    listener:   (msg: Message) => void;
  }

  /** Options for {@link Client.declareExchange}. */
  export interface DeclareExchangeOptions {
    name:        string;
    kind:        ExchangeKind;
    durable?:    boolean;
    autoDelete?: boolean;
    /** Declare as an internal exchange (not reachable by publishers). */
    internal?:   boolean;
    noWait?:     boolean;
    args?:       Record<string, unknown>;
  }

  /** Options for {@link Client.bindExchange}. */
  export interface BindExchangeOptions {
    /** Source exchange name. */
    source:      string;
    /** Destination exchange name. */
    destination: string;
    routingKey?: string;
    noWait?:     boolean;
    args?:       Record<string, unknown>;
  }

  /**
   * Options for {@link Client.unbindExchangeWithOptions}.
   * Use this instead of {@link Client.unbindExchange} for AMQP 0.9.1.
   */
  export interface UnbindExchangeOptions {
    source:      string;
    destination: string;
    routingKey?: string;
    args?:       Record<string, unknown>;
  }

  /** Options for {@link Client.declareQueue}. */
  export interface DeclareQueueOptions {
    name:              string;
    durable?:          boolean;
    /** Delete the queue when the last consumer unsubscribes. */
    deleteWhenUnused?: boolean;
    exclusive?:        boolean;
    noWait?:           boolean;
    args?:             Record<string, unknown>;
  }

  /** Options for {@link Client.bindQueue}. */
  export interface BindQueueOptions {
    queueName:    string;
    exchangeName: string;
    routingKey?:  string;
    noWait?:      boolean;
    args?:        Record<string, unknown>;
  }

  /**
   * Options for {@link Client.unbindQueueWithOptions}.
   * Use this instead of {@link Client.unbindQueue} for AMQP 0.9.1.
   */
  export interface UnbindQueueOptions {
    queueName:    string;
    exchangeName: string;
    routingKey?:  string;
    args?:        Record<string, unknown>;
  }

  /** Options passed to the {@link Client} constructor. */
  export interface ClientOptions {
    connectionOptions: ConnectionOptions;
    /**
     * Protocol-level options forwarded to the underlying `amqp091-go` Config.
     * Commonly used for TLS configuration.
     */
    options?: Record<string, unknown>;
  }

  /**
   * AMQP 0.9.1 client. Each k6 VU that constructs a Client gets its own
   * isolated connection; the connection is established lazily on first I/O.
   *
   * @example
   * ```typescript
   * import { Client } from 'k6/x/amqp091';
   *
   * const client = new Client({
   *   connectionOptions: { host: 'localhost', port: 5672, username: 'guest', password: 'guest' },
   * });
   * ```
   */
  export class Client {
    constructor(opts: ClientOptions);

    /** Publish a message synchronously. */
    publish(options: PublishOptions): void;

    /**
     * Publish a message asynchronously using publisher confirms.
     * The returned promise resolves when the broker acknowledges delivery,
     * and rejects if the broker nacks the message or the VU context is cancelled.
     */
    publishAsync(options: PublishOptions): Promise<void>;

    /**
     * Start consuming messages from a queue.
     * The `listener` callback is invoked on the VU event loop (safe for k6 metrics etc.).
     */
    listen(options: ListenOptions): void;

    /** Declare an exchange. */
    declareExchange(options: DeclareExchangeOptions): void;
    /** Delete an exchange. */
    deleteExchange(name: string): void;
    /** Bind a destination exchange to a source exchange. Returns an empty string (0.9.1 has no binding path). */
    bindExchange(options: BindExchangeOptions): string;
    /**
     * Not supported for AMQP 0.9.1 — always throws.
     * Use {@link unbindExchangeWithOptions} instead.
     * @throws {Error}
     */
    unbindExchange(bindingPath: string): void;
    /** Remove an exchange binding using explicit source/destination/key parameters. */
    unbindExchangeWithOptions(options: UnbindExchangeOptions): void;

    /** Declare a queue. */
    declareQueue(options: DeclareQueueOptions): void;
    /** Delete a queue. */
    deleteQueue(name: string): void;
    /** Bind a queue to an exchange. Returns an empty string (0.9.1 has no binding path). */
    bindQueue(options: BindQueueOptions): string;
    /**
     * Not supported for AMQP 0.9.1 — always throws.
     * Use {@link unbindQueueWithOptions} instead.
     * @throws {Error}
     */
    unbindQueue(bindingPath: string): void;
    /** Remove a queue binding using explicit queue/exchange/key parameters. */
    unbindQueueWithOptions(options: UnbindQueueOptions): void;
    /** Purge all messages from a queue. Returns the number of messages removed. */
    purgeQueue(name: string): number;
    /** Return metadata about a queue without modifying it. */
    inspectQueue(name: string): QueueInfo;

    /** Stop all consumer goroutines and close the AMQP connection. */
    close(): void;
  }
}

// ── k6/x/amqp (backward-compatible alias for k6/x/amqp091) ──────────────────

declare module 'k6/x/amqp' {
  export * from 'k6/x/amqp091';
}

// ── k6/x/amqp10 ──────────────────────────────────────────────────────────────

declare module 'k6/x/amqp10' {
  /** AMQP exchange type. */
  export type ExchangeKind = 'direct' | 'topic' | 'fanout' | 'headers';

  /** RabbitMQ queue variant. Quorum and Stream are AMQP 1.0 only. */
  export type QueueType = 'classic' | 'quorum' | 'stream';

  /**
   * A received AMQP 1.0 message passed to the {@link ListenOptions.listener} callback.
   *
   * `accept` and `discard` are meaningful only when `autoAck` is `false`.
   * When `autoAck` is `true` the extension calls `accept` automatically after
   * the listener returns without error.
   *
   * **Note:** AMQP 1.0 has no concept of requeuing; the `requeue` parameter
   * of `discard` is accepted but ignored.
   */
  export interface Message {
    /** Message payload decoded as a UTF-8 string. */
    body:            string;
    /** Maps to AMQP 1.0 Properties.Subject. */
    routingKey?:     string;
    /** MIME content type from AMQP 1.0 Properties.ContentType. */
    contentType?:    string;
    /** Correlation identifier from AMQP 1.0 Properties.CorrelationID. */
    correlationId?:  string;
    /** Application properties attached to the message. */
    headers?:        Record<string, unknown>;
    /** Message identifier from AMQP 1.0 Properties.MessageID. */
    messageId?:      string;
    /**
     * Acknowledge (accept) the message.
     * Only meaningful when `autoAck` is `false`.
     */
    accept(): void;
    /**
     * Reject (discard) the message. The `requeue` parameter is ignored for AMQP 1.0.
     * Only meaningful when `autoAck` is `false`.
     */
    discard(requeue?: boolean): void;
  }

  /** Options used to establish a broker connection. */
  export interface ConnectionOptions {
    host:     string;
    port:     number;
    username: string;
    password: string;
  }

  /**
   * Queue metadata returned by {@link Client.inspectQueue}.
   * Shape matches `rabbitmqamqp.QueueInfo`.
   */
  export interface QueueInfo {
    name:      string;
    messages:  number;
    consumers: number;
  }

  /** Options for {@link Client.publish}. */
  export interface PublishOptions {
    /** Publish directly to a queue. Mutually exclusive with `exchange`. */
    queueName?:     string;
    /** Publish to an exchange. Mutually exclusive with `queueName`. */
    exchange?:      string;
    /** Routing key used when publishing to an exchange. */
    routingKey?:    string;
    /** Message body. */
    body?:          string | ArrayBuffer;
    /** MIME content type. Maps to AMQP 1.0 Properties.ContentType. */
    contentType?:   string;
    /** Set the Durable flag on the AMQP 1.0 message header. */
    persistent?:    boolean;
    /** Maps to AMQP 1.0 Properties.Subject. */
    subject?:       string;
    /** Maps to AMQP 1.0 Properties.CorrelationID. */
    correlationId?: string;
    /** Maps to AMQP 1.0 Properties.MessageID. */
    messageId?:     string;
  }

  /** Options for {@link Client.listen}. */
  export interface ListenOptions {
    /** Queue to consume from. */
    queueName:        string;
    /** Receiver link name. */
    consumer?:        string;
    /** Automatically accept (ack) messages after the listener returns without error. */
    autoAck?:         boolean;
    /** AMQP 1.0 link flow control credits. Defaults to 256. */
    initialCredits?:  number;
    /**
     * Called for each received message.
     * When `autoAck` is `false`, call `msg.accept()` or `msg.discard()` to
     * acknowledge or reject the message explicitly.
     */
    listener:         (msg: Message) => void;
  }

  /** Options for {@link Client.declareExchange}. */
  export interface DeclareExchangeOptions {
    name:        string;
    kind:        ExchangeKind;
    autoDelete?: boolean;
    /** Additional arguments passed to the management API. */
    args?:       Record<string, unknown>;
  }

  /**
   * Options for {@link Client.bindExchange}.
   *
   * Both AMQP 1.0 native names and 0.9.1-style aliases are accepted.
   * Native names take precedence when both are provided.
   */
  export interface BindExchangeOptions {
    /** Source exchange name (AMQP 1.0 native). */
    sourceExchange?:      string;
    /** Destination exchange name (AMQP 1.0 native). */
    destinationExchange?: string;
    /** Binding key (AMQP 1.0 native). */
    bindingKey?:          string;
    /** Alias for `sourceExchange` (0.9.1 style). */
    source?:              string;
    /** Alias for `destinationExchange` (0.9.1 style). */
    destination?:         string;
    /** Alias for `bindingKey` (0.9.1 style). */
    routingKey?:          string;
    args?:                Record<string, unknown>;
  }

  /** Options for {@link Client.declareQueue}. */
  export interface DeclareQueueOptions {
    name:        string;
    /** Queue type; defaults to `'classic'`. */
    queueType?:  QueueType;
    autoDelete?: boolean;
    exclusive?:  boolean;
    /** Additional arguments (e.g. stream retention settings). */
    args?:       Record<string, unknown>;
  }

  /**
   * Options for {@link Client.bindQueue}.
   *
   * Both AMQP 1.0 native names and 0.9.1-style aliases are accepted.
   * Native names take precedence when both are provided.
   */
  export interface BindQueueOptions {
    /** Source exchange name (AMQP 1.0 native). */
    sourceExchange?:   string;
    /** Destination queue name (AMQP 1.0 native). */
    destinationQueue?: string;
    /** Binding key (AMQP 1.0 native). */
    bindingKey?:       string;
    /** Alias for `sourceExchange` (0.9.1 style). */
    exchangeName?:     string;
    /** Alias for `destinationQueue` (0.9.1 style). */
    queueName?:        string;
    /** Alias for `bindingKey` (0.9.1 style). */
    routingKey?:       string;
    args?:             Record<string, unknown>;
  }

  /** Options passed to the {@link Client} constructor. */
  export interface ClientOptions {
    connectionOptions: ConnectionOptions;
    /**
     * Protocol-level options forwarded to the underlying `AmqpConnOptions`.
     * Commonly used for TLS configuration.
     */
    options?: Record<string, unknown>;
  }

  /**
   * AMQP 1.0 client. Each k6 VU that constructs a Client gets its own
   * isolated connection; the connection is established lazily on first I/O.
   *
   * @example
   * ```typescript
   * import { Client } from 'k6/x/amqp10';
   *
   * const client = new Client({
   *   connectionOptions: { host: 'localhost', port: 5672, username: 'guest', password: 'guest' },
   * });
   * ```
   */
  export class Client {
    constructor(opts: ClientOptions);

    /** Publish a message synchronously. Either `queueName` or `exchange` must be set. */
    publish(options: PublishOptions): void;

    /**
     * Start consuming messages from a queue.
     * The `listener` callback is invoked on the VU event loop (safe for k6 metrics etc.).
     */
    listen(options: ListenOptions): void;

    /** Declare an exchange. */
    declareExchange(options: DeclareExchangeOptions): void;
    /** Delete an exchange. */
    deleteExchange(name: string): void;
    /** Bind a destination exchange to a source exchange. Returns the binding path required by {@link unbindExchange}. */
    bindExchange(options: BindExchangeOptions): string;
    /** Remove an exchange binding identified by the path returned from {@link bindExchange}. */
    unbindExchange(bindingPath: string): void;

    /** Declare a queue. */
    declareQueue(options: DeclareQueueOptions): void;
    /** Delete a queue. */
    deleteQueue(name: string): void;
    /** Bind a queue to an exchange. Returns the binding path required by {@link unbindQueue}. */
    bindQueue(options: BindQueueOptions): string;
    /** Remove a queue binding identified by the path returned from {@link bindQueue}. */
    unbindQueue(bindingPath: string): void;
    /** Purge all messages from a queue. Returns the number of messages removed. */
    purgeQueue(name: string): number;
    /** Return metadata about a queue without modifying it. */
    inspectQueue(name: string): QueueInfo;

    /** Stop all consumer goroutines and close the AMQP 1.0 connection. */
    close(): void;
  }
}

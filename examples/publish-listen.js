// Example: publish and listen using AMQP 0.9.1 (RabbitMQ classic).
// Swap the import to 'k6/x/amqp10' to use AMQP 1.0 with the same API shape.
//
// Note: listen() is called in default() (not setup()) so that the JS listener
// callback runs on each VU's own event loop (dispatched during sleep()).
import {Client} from 'k6/x/amqp';
import {sleep} from 'k6';

export const options = {
    stages: [
        {duration: '3m', target: 10},
        {duration: '5m', target: 10},
        {duration: '10m', target: 35},
        {duration: '3m', target: 0},
    ],
};

// Clients are created in the init context (no IO yet) and connected lazily.
const client = new Client({
    connectionOptions: {
        host: 'localhost',
        port: 5672,
        username: 'guest',
        password: 'guest',
    },
});

export function setup() {
    // Declare resources once before the test starts.
    client.declareExchange({name: 'k6-exchange', kind: 'topic', durable: true});
    client.declareQueue({name: 'k6-queue', durable: true});
    client.bindQueue({queueName: 'k6-queue', exchangeName: 'k6-exchange', routingKey: 'k6'});
}

// Per-VU flag: each VU starts its consumer once on the first iteration.
let _listening = false;

export default function () {
    if (!_listening) {
        client.listen({
            queueName: 'k6-queue',
            autoAck: true,
            listener: (msg) => {
                console.log('Received:', msg.body);
            },
        });
        _listening = true;
    }

    client.publish({
        exchange: 'k6-exchange',
        routingKey: 'k6',
        contentType: 'text/plain',
        body: 'Ping from k6',
    });

    sleep(0.01);
}

export function teardown() {
    client.deleteQueue('k6-queue');
    client.deleteExchange('k6-exchange');
    client.close();
}

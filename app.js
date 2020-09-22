const amqplib = require('amqplib');
const Broker = require('rascal').BrokerAsPromised;
const config = require('./config.json');

var assert = require('assert');
var util = require('util');


var rabbit_user= process.env.RABBITMQ_USER;
var rabbit_pwd = process.env.RABBITMQ_PWD;
var rabbit_host = process.env.RABBITMQ_HOST;
var rabbit_port = process.env.RABBITMQ_PORT;
var vhost = process.env.RABBIT_VHOST;
assert(rabbit_user);
assert(rabbit_pwd);
assert(rabbit_host);
assert(rabbit_port);
assert(vhost);

var amql_url = util.format("amqp://%s:%s@%s:%s/%s", rabbit_user, rabbit_pwd, rabbit_host, rabbit_port, vhost);

async function rascal_produce(){
    console.log("Publishing");
    var msg = 'Hello World!';
    const broker = await Broker.create(config);
    broker.on('error', console.error);
    const publication = await broker.publish('demo_publication', msg);
    publication.on('error', console.error);
    console.log("Published")
}

async function produce(){
    console.log("Publishing");
    var conn = await amqplib.connect(amql_url, "heartbeat=60");
    var ch = await conn.createChannel()
    var exch = 'test_exchange';
    var q = 'test_queue';
    var rkey = 'test_route';
    var msg = 'Hello World!';
    await ch.assertExchange(exch, 'direct', {durable: true}).catch(console.error);
    await ch.assertQueue(q, {durable: true});
    await ch.bindQueue(q, exch, rkey);
    await ch.publish(exch, rkey, Buffer.from(msg));
    setTimeout( function()  {
        ch.close();
        conn.close();},  500 );
}

async function consume() {
    var conn = await amqplib.connect(amql_url, "heartbeat=60");
    var ch = await conn.createChannel()
    var q = 'test_queue';
    await conn.createChannel();
    await ch.assertQueue(q, {durable: true});
    await ch.consume(q, function (msg) {
        console.log(msg.content.toString());
        ch.ack(msg);
        ch.cancel('myconsumer');
        }, {consumerTag: 'myconsumer'});
    setTimeout( function()  {
        ch.close();
        conn.close();},  500 );
}

async function rascal_consume(){
    console.log("Consuming");
    const broker = await Broker.create(config);
    broker.on('error', console.error);
    const subscription = await broker.subscribe('demo_subscription', 'b1');
    subscription.on('message', (message, content, ackOrNack)=>{
        console.log(content);
        ackOrNack();
        subscription.cancel();
    });
    subscription.on('error', console.error);
}

async function main() {
    //await produce().catch(console.error);
    //await consume().catch(console.error);
    await rascal_produce().catch(console.error);
    await rascal_consume().catch(console.error)
}

main();

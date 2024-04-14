const amqp = require('amqplib');

const url = "amqp://user:password@localhost:5672";

const weExchange = "WE_EXCHANGE";
const wmExchange = "WM_EXCHANGE";
const dlExchange = "DL_EXCHANGE";

const we1Queue = "WE1_QUEUE";
const we2Queue = "WE2_QUEUE";
const we3Queue = "WE3_QUEUE";
const wm1Queue = "WM1_QUEUE";
const wm2Queue = "WM2_QUEUE";


const wm1 = "1";
const wm2 = "2";
const we1 = "1";
const we2 = "2";
const we3 = "3";

let dlMap = {
    "1": "2",
    "2": "1"
}

async function sendMessage(exchange, routingKey, msg){
    try {
        const connection = await amqp.connect(url);
        const channel = await connection.createChannel();
        await channel.assertExchange(exchange, "direct");
        await channel.publish(exchange,routingKey,Buffer.from(msg));
        await channel.close();
        await connection.close();

    } catch (error) {
        console.error(
          error, msg, exchange, routingKey  
        );
    }
}

async function receiveMessage(exchange, queue, routingKey) {
    try{
        const connection = await amqp.connect(url);
        const channel = await connection.createChannel();
        await channel.assertExchange(exchange, "direct");
        await channel.assertQueue(queue);
        await channel.bindQueue(queue, exchange, routingKey);
        return new Promise((resolve, reject) => {
            channel.consume(queue, (msg) => {
                if (msg) {
                    const decodedMsg = msg.content.toString();
                    console.log(`Received for ${exchange} ${queue} ${routingKey} ${decodedMsg}`);
                    channel.ack(msg);
                    resolve(decodedMsg + ` received in ${queue}`);
                }
            });
        });
    }
    catch (error) {
        console.error(error, msg, exchange, routingKey);
    }
}

async function main() {
    // WM sending messages to WE pods in round robin fashion
await sendMessage(weExchange, we2, "Hi from WM1 to WE2");
await sendMessage(weExchange, we3, "Hi from WM2 to WE3");
await sendMessage(weExchange, we1, "Hi from WM1 to WE1");

// WE receiving messages from WM pods
let msg1 = await receiveMessage(weExchange, we1Queue, we1);
let msg2 = await receiveMessage(weExchange, we2Queue, we2);
let msg3 = await receiveMessage(weExchange, we3Queue, we3);

//WE pods sending status updates back to WM pods
await sendMessage(wmExchange, wm2, msg2);
await sendMessage(wmExchange, wm1, msg1);

// looping over to simulate multiple nodes
for (let i = 0; i<3; i++){
    //perform node operations
    await sendMessage(wmExchange, wm1, msg3+` order ${i}`);
}

await receiveMessage(wmExchange, wm1Queue, wm1);
await receiveMessage(wmExchange, wm2Queue, wm2);
}

main();

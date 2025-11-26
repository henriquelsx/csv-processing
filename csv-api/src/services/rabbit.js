const amqp = require("amqplib");

let channel = null;

async function connectRabbit() {
  if (channel) return channel;

  const connection = await amqp.connect(process.env.RABBITMQ_URL);
  channel = await connection.createChannel();

  await channel.assertQueue(process.env.RABBITMQ_QUEUE, { durable: true });

  console.log("RabbitMQ conectado!");

  return channel;
}

module.exports = connectRabbit;

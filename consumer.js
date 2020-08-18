const { Kafka } = require('kafkajs');

const kafka = new Kafka({
  clientId: 'lambda identifier',
  brokers: ['']
});

const consumer = kafka.consumer({ groupId: 'brendan-test-group' + Math.random() });

const consume = async () => {
  // Consuming
  await consumer.subscribe({ topic: 'event-firehose', fromBeginning: true });
  await consumer.connect();

  await consumer.run({
    eachMessage: async ({ topic, partition, message }) => {
      console.log(JSON.parse(message.value.toString()).event_type);
      // const event_type = JSON.parse(message.value.toString()).event_type;
      const event_name = JSON.parse(message.value.toString()).event_name;
      console.log({
        event_name,
        topic,
        partition,
        offset: message.offset,
        value: message.value.toString(),
      });
    },
  });
};

consume().catch(console.error);

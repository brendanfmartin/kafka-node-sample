const { Kafka } = require('kafkajs');

const kafka = new Kafka({
  clientId: 'notiengine',
  brokers: ['kafka-dev.relaystaging.com:9092'],
  requestTimeout: 25000
});

const producer = kafka.producer();
const logData = { };


const logEventToKafka = (innerLogData) => {
  const record = {
    topic: 'event-firehose-test-a',
    messages: [{value: JSON.stringify(innerLogData)}],
  };
  return new Promise((resolve, reject) => {
    return producer.connect()
      .then(() => producer.connect())
      .then(() => producer.connect())
      .then(() => producer.connect())
      .then(() => producer.connect())
      .then(() => producer.send(record))
      .then(() => resolve('Success: Kafka'))
      .catch((error) => {
        console.error('test.kafka.failure', {
          message: 'failed to deliver to kafka',
          error,
          logData
        });
        reject(new Error('Kafka'));
      });
  });
};

logEventToKafka(logData)
  .then((r) => {
    console.log('r', r);
    process.exit(0)
  })
  .catch((e) => {
    console.log('e', e)
    process.exit(-1)
  });

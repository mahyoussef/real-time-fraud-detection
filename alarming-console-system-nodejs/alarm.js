const { Kafka } = require('kafkajs');

const kafka = new Kafka({
  clientId: 'my-app',
  brokers: ['localhost:9092']
});


async function fireAlarmAsync()
{
  const consumer = kafka.consumer({ groupId: 'alarm-group' });

  await consumer.connect();
  await consumer.subscribe({ topic: 'alarms', fromBeginning: true });

  await consumer.run({
    eachMessage: async ({ topic, partition, message }) => {
      console.log({
        value: message.value.toString(),
      })
    },
  });
  
}

fireAlarmAsync();



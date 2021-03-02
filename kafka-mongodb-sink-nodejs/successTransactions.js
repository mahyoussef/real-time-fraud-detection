const { Kafka } = require('kafkajs');
const kafka = new Kafka({ clientId: 'my-app', brokers: ['localhost:9092']});

const mongoose = require('mongoose');
mongoose.connect('mongodb://localhost:27017/sales', {useNewUrlParser: true, useUnifiedTopology: true});

const db = mongoose.connection;
db.on('error', console.error.bind(console, 'connection error:'));
db.once('open', function() {
  // we're connected!
});

const transactionSchema = new mongoose.Schema({
  transactionId: String,
  timestamp: String,
  city: String,
  customerId: String,
  accountLink: String,
  creditCardNumber: String,
  transactionAmount: Number
});

const Transaction = mongoose.model('Transaction', transactionSchema);

async function insertTransactionsAsync()
{
  const consumer = kafka.consumer({ groupId: 'valid-transactions' });

  await consumer.connect();
  await consumer.subscribe({ topic: 'success', fromBeginning: false });

  await consumer.run({
    eachMessage: async ({ topic, partition, message }) => {
      console.log({
        value: message.value.toString(),
      });
      
      
      console.log("before parse");
      const resultArray = message.value.toString().split(",");
      console.log(resultArray);
             console.log("before convert");
      const transaction = new Transaction({
       "transactionId": resultArray[0],
        "timestamp": resultArray[1],
        "city" : resultArray[2],
        "customerId": resultArray[3],
        "accountLink": resultArray[4],
        "creditCardNumber": resultArray[5],
        "transactionAmount": resultArray[6] 
       
       });
       console.log("before save");
       transaction.save(function (err, transaction) {
       	console.log("inside save");
       	console.log(transaction);
    		if (err) return console.error(err);
  	});
  
    },
  }); 
}

insertTransactionsAsync();

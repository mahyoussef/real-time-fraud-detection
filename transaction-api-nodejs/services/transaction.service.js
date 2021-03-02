const express = require('express')

const { Kafka } = require('kafkajs')

const kafka = new Kafka({
  clientId: 'my-app',
  brokers: ['localhost:9092']
})

const producer = kafka.producer()

exports.getTransaction = async (req, res) => {
    const transaction = {
            'Number 9': 1.99,
            'Number 9 Large': 2.99,
            'Number 6 with Extra Dip': 3.25,
            'Number 7': 3.99,
            'Number 45': 3.45
        }

        return res.status(200).json({ transaction: transaction })
};

exports.createTransaction = async (req, res) => {
    const transaction = { 
        "transactionId": req.body.transactionId,
        "timestamp": req.body.timestamp,
        "city" : req.body.city,
        "customerId": req.body.customerId,
        "accountLink": req.body.accountLink,
        "creditCardNumber": req.body.creditCardNumber,
        "transactionAmount": req.body.transactionAmount
        
        }
        const resultString = req.body.transactionId + "," + req.body.timestamp + "," + req.body.city + "," + req.body.customerId + "," + req.body.accountLink +
        "," + req.body.creditCardNumber + "," + req.body.transactionAmount;
        
        await producer.connect()
	 
	 await producer.send({
  		topic: 'transactions',
  		messages: [ { value: resultString }]
  		})
			
	await producer.disconnect()
	
        return res.status(200).json({ transaction: transaction })
};



const express = require('express')
const router = express.Router()

const transactionService = require('../services/transaction.service')

router.get('/', transactionService.getTransaction);
router.post('/', transactionService.createTransaction);

module.exports = router

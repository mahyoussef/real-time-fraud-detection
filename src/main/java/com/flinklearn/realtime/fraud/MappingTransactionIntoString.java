package com.flinklearn.realtime.fraud;

import com.flinklearn.realtime.pojo.Transaction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;

public class MappingTransactionIntoString implements MapFunction
<Tuple2<Transaction, String>, String>{

    @Override
    public String map(Tuple2<Transaction, String> transaction) throws Exception {
        return transaction.f0.TransactionId + "," + transaction.f0.Timestamp
                + "," + transaction.f0.City + "," + transaction.f0.CustomerId
                + "," + transaction.f0.AccountLink + "," + transaction.f0.CreditCardNumber
                + "," + transaction.f0.TransactionAmount;
    }
}

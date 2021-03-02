package com.flinklearn.realtime.fraud;

import com.flinklearn.realtime.common.Utils;
import com.flinklearn.realtime.pojo.Transaction;
import io.lettuce.core.RedisClient;
import io.lettuce.core.RedisFuture;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.api.async.RedisStringAsyncCommands;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.async.AsyncFunction;
import org.apache.flink.streaming.api.functions.async.ResultFuture;
import org.apache.flink.streaming.api.functions.async.RichAsyncFunction;

import java.util.Collections;
import java.util.concurrent.ExecutionException;

public class CheckingValidityOfTransactionAsync extends RichAsyncFunction
        <Transaction, Tuple2<Transaction, String>>
        implements AsyncFunction<Transaction, Tuple2<Transaction, String>> {

    private transient RedisClient client;
    private StatefulRedisConnection<String, String> asyncRedisConnection;
    private RedisStringAsyncCommands<String, String> async;
    @Override
    public void open(Configuration parameters){
        client = RedisClient.create("redis://"+ Utils.REDIS_IP_ADDRESS);
        asyncRedisConnection = client.connect();
        async = asyncRedisConnection.async();
    }
    @Override
    public void close(){
        asyncRedisConnection.close();
    }
    @Override
    public void timeout(final Transaction transaction,
                        final ResultFuture<Tuple2<Transaction, String>> resultFuture)
    {
        resultFuture.complete(Collections.singleton
                (new Tuple2(transaction, Utils.REDIS_FAILED)));
    }
    @Override
    public void asyncInvoke(Transaction transaction, ResultFuture<Tuple2<Transaction, String>> resultFuture) throws Exception {
        try {
            // Valid Transaction
            if (!isStolenCreditCard(transaction.CreditCardNumber) && !isCustomerAlarmed(transaction.CustomerId)) {
                resultFuture.complete(Collections.singleton
                        (new Tuple2(transaction, Utils.VALID_TRANSACTION)));
            }
            // invalid Transaction
            resultFuture.complete(Collections.singleton
                    (new Tuple2(transaction, Utils.INVALID_TRANSACTION)));
        }
        catch (Exception ex)
        {
            resultFuture.complete(Collections.singleton
                    (new Tuple2(transaction, Utils.REDIS_FAILED)));
        }
    }

    private boolean isCustomerAlarmed(String customerId) throws ExecutionException, InterruptedException {
        return redisHasValue(customerId);
    }

    private boolean redisHasValue(String customerId) throws ExecutionException, InterruptedException {
        RedisFuture<String> resultFuture = async.get(customerId);
        String redisResult = resultFuture.get();
        if (redisResult == null)
            return false;

        return true;
    }

    private boolean isStolenCreditCard(String creditCardNumber) throws ExecutionException, InterruptedException {
        return redisHasValue(creditCardNumber);
    }
}

package com.flinklearn.realtime.fraud;



import com.flinklearn.realtime.common.Utils;
import com.flinklearn.realtime.factory.KafkaBuilder;
import com.flinklearn.realtime.pojo.Transaction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.AsyncDataStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.types.Row;

import java.util.concurrent.TimeUnit;

public class FraudDetectionRealTime {
    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment environment = StreamExecutionEnvironment
                .getExecutionEnvironment();
        /****************************************************************************
         *                 Kafka Consumer.
         ****************************************************************************/
        FlinkKafkaConsumer<String> kafkaTransactionsConsumer = KafkaBuilder
                .createFlinkKafkaConsumer(Utils.TOPIC_TRANSACTIONS);
        /****************************************************************************
         *                 Stream Input.
         ****************************************************************************/
        Integer kafkaConsumerParallelism = 1;
        DataStream<String> kafkaStream = environment.addSource(kafkaTransactionsConsumer)
                .setParallelism(kafkaConsumerParallelism);

        DataStream<Transaction> transactionStream = kafkaStream.map(t -> new Transaction(t));
        /****************************************************************************
         *                 Kafka Producer.
         ****************************************************************************/
        FlinkKafkaProducer<String> kafkaAlarmingProducer = KafkaBuilder
                .createFlinkKafkaProducer(Utils.TOPIC_ALARMS);
        FlinkKafkaProducer<String> kafkaSuccessProducer = KafkaBuilder
                .createFlinkKafkaProducer(Utils.TOPIC_SUCCESS);
        /****************************************************************************
         *                 Flink Operations.
         ****************************************************************************/
        DataStream<Tuple2<Transaction, String>> labeledStream
                = AsyncDataStream.unorderedWait(transactionStream,
                new CheckingValidityOfTransactionAsync(), 5000, TimeUnit.MILLISECONDS, 100);

        DataStream<Tuple2<Transaction, String>> validTransactions = labeledStream
                .filter(t -> t.f1.equalsIgnoreCase(Utils.VALID_TRANSACTION));

        DataStream<String> stolenCardsOrAlarmedCustomers = labeledStream
                .filter(t -> t.f1.equalsIgnoreCase(Utils.INVALID_TRANSACTION))
                .map(new MappingTransactionIntoString());

        DataStream<Tuple2<String, Transaction>>
                keyedValidTransactions = validTransactions
                .map(new MapFunction<Tuple2<Transaction, String>, Tuple2<String, Transaction>>() {
                    @Override
                    public Tuple2<String, Transaction> map(Tuple2<Transaction, String> transaction) throws Exception {
                        return new Tuple2(transaction.f0.CustomerId, transaction.f0);
                    }
                });

        DataStream<Tuple2<Transaction, String>> bulkOfTransactions =
                keyedValidTransactions
                .map(new MapFunction<Tuple2<String, Transaction>, Tuple3<String, Transaction, Integer>>() {
                    @Override
                    public Tuple3<String, Transaction, Integer> map(Tuple2<String, Transaction> transaction) throws Exception {
                        return new Tuple3(transaction.f0, transaction.f1, 1);
                    }
                })
                .keyBy(t -> t.f0)
                .window(TumblingProcessingTimeWindows.of(Time.seconds(10)))
                .sum(2)
                .flatMap(new CheckingMoreThanTenTransactionsPerWindow());

        DataStream<String> invalidBuildOfTransactions
                = bulkOfTransactions.filter(t -> t.f1.equalsIgnoreCase(Utils.INVALID_TRANSACTION))
                .map(new MappingTransactionIntoString());

        DataStream<Transaction> validBulkOfTransactions
                = bulkOfTransactions.filter(t -> t.f1.equalsIgnoreCase(Utils.VALID_TRANSACTION))
                .map(t -> t.f0);

        DataStream<Tuple2<Transaction, String>> freqCityChangeTransactions
                = keyedValidTransactions
                .keyBy(t -> t.f0)
                .window(TumblingProcessingTimeWindows.of(Time.seconds(10)))
                .process(new FreqCityChangeTransactionProcess());

        DataStream<String> invalidFreqCityChangeTransactions
                = freqCityChangeTransactions
                .filter(t -> t.f1.equalsIgnoreCase(Utils.INVALID_TRANSACTION))
                .map(new MappingTransactionIntoString());

        DataStream<String> totalValidTransactions
                = freqCityChangeTransactions
                .filter(t -> t.f1.equalsIgnoreCase(Utils.VALID_TRANSACTION))
                .map(new MappingTransactionIntoString());

        DataStream<String> invalidTransactions
                = stolenCardsOrAlarmedCustomers
                .union(invalidBuildOfTransactions, invalidFreqCityChangeTransactions);
        /****************************************************************************
         *                 Flink Kafka Output.
         ****************************************************************************/
        Integer kafkaProducerParallelism = 1;
        invalidTransactions.addSink(kafkaAlarmingProducer).setParallelism(kafkaProducerParallelism);
        totalValidTransactions.addSink(kafkaSuccessProducer).setParallelism(kafkaProducerParallelism);

        environment.execute(Utils.JOB_NAME);
    }
}

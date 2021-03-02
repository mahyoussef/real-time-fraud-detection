package com.flinklearn.realtime.fraud;

import com.flinklearn.realtime.common.Utils;
import com.flinklearn.realtime.pojo.Transaction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.util.Collector;

public class CheckingMoreThanTenTransactionsPerWindow implements FlatMapFunction
        <Tuple3<String, Transaction, Integer>, Tuple2<Transaction, String>> {
    @Override
    public void flatMap(Tuple3<String, Transaction, Integer> input,
                        Collector<Tuple2<Transaction, String>> output) throws Exception {
        if(input.f2 > 10)
            output.collect(new Tuple2(input.f1, Utils.INVALID_TRANSACTION));

        output.collect(new Tuple2(input.f1, Utils.VALID_TRANSACTION));

    }
}


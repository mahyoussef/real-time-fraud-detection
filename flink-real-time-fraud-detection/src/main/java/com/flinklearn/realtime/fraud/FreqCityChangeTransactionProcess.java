package com.flinklearn.realtime.fraud;

import com.flinklearn.realtime.common.Utils;
import com.flinklearn.realtime.pojo.Transaction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

public class FreqCityChangeTransactionProcess extends ProcessWindowFunction
        <Tuple2<String, Transaction>, Tuple2<Transaction, String>, String, TimeWindow> {

    @Override
    public void process(String key, Context context,
                        Iterable<Tuple2<String, Transaction>> inputs,
                        Collector<Tuple2<Transaction, String>> output) throws Exception {
        String lastCity = "";
        Integer changeCount = 0;
        for(Tuple2<String, Transaction> input : inputs)
        {
            String city = input.f1.City;
            if(lastCity.isEmpty())
            {
                lastCity = city;
            }
            else
            {
                if(!city.equals(lastCity))
                {
                    lastCity = city;
                    changeCount +=1;
                }
            }
            if(changeCount > 2)
            {
                System.out.println("Inside Changing Count");
                output.collect(new Tuple2(input.f1, Utils.INVALID_TRANSACTION));
            }
        }
        inputs.forEach(t ->
        {
            System.out.println("inside for loop");
            output.collect(new Tuple2(t.f1, Utils.VALID_TRANSACTION));
        });
    }
}
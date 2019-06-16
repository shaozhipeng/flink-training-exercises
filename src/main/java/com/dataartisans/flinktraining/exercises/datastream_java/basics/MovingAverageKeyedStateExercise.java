package com.dataartisans.flinktraining.exercises.datastream_java.basics;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.LinkedList;

/**
 * Keyed ValueState Example
 */
public class MovingAverageKeyedStateExercise {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStream<String> text = env.socketTextStream("localhost", 19000, "\n");
        DataStream<Tuple2<String, Double>> input = text.map(new MapFunction<String, Tuple2<String, Double>>() {
            @Override
            public Tuple2<String, Double> map(String s) throws Exception {
                // S001 2.0
                // S001 5.0
                // S001 9.0
                // nc -l 19000
                String[] record = s.split("\\W+");
                return new Tuple2<>(record[0], Double.valueOf(record[1]));
            }
        });
        DataStream<Tuple2<String, Double>> smoothed = input.keyBy(0).map(new Smoother());
        smoothed.print();
        /**
         * 2> (S001,0.0)
         * 2> (S001,0.0)
         * 2> (S001,7.0)
         */

        env.execute("MovingAverageKeyedStateExercise Job");
    }

    public static class Smoother extends RichMapFunction<Tuple2<String, Double>, Tuple2<String, Double>> {
        private ValueState<MovingAverage> averageState;

        @Override
        public void open(Configuration conf) {
            ValueStateDescriptor<MovingAverage> descriptor =
                    new ValueStateDescriptor<>("moving average", MovingAverage.class);
            averageState = getRuntimeContext().getState(descriptor);
        }

        @Override
        public Tuple2<String, Double> map(Tuple2<String, Double> item) throws Exception {
            // access the state for this key
            MovingAverage average = averageState.value();

            // create a new MovingAverage (with window size 2) if none exists for this key
            if (average == null) {
                average = new MovingAverage(2);
            }

            // add this event to the moving average
            average.add(item.f1);
            averageState.update(average);

            // return the smoothed result
            return new Tuple2(item.f0, average.getAverage());
        }
    }

}

class MovingAverage {
    double sum;
    int size;
    LinkedList<Double> list;
    double average;

    /**
     * 构造方法
     *
     * @param size
     */
    public MovingAverage(int size) {
        this.list = new LinkedList<>();
        this.size = size;
    }

    public double add(double val) {
        sum += val;
        list.offer(val);
        if (list.size() <= size) {
            return sum / list.size();
        }
        sum -= list.poll();
        return average = sum / size;
    }

    public double getAverage() {
        return average;
    }
}

package com.dataartisans.flinktraining.exercises.datastream_java.state;

import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.RichCoFlatMapFunction;
import org.apache.flink.util.Collector;

import java.io.Serializable;

public class AJoinBExerise {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStream<Tuple2<String, String>> a = env.fromElements(
                new Tuple2<String, String>("ss", "StringString"),
                new Tuple2<String, String>("ls", "list")
        ).keyBy(0);

        DataStream<Tuple3<String, Integer, Double>> b = env.fromElements(
                new Tuple3<String, Integer, Double>("ss", new Integer(2), new Double(3.3)),
                new Tuple3<String, Integer, Double>("ls", new Integer(5), new Double(3.9))
        ).keyBy(0);

        DataStream<Tuple4<String, String, Integer, Double>> res = a.connect(b)
                .flatMap(new ABEnrichmentFunction());
        res.print();

        /**
         * 1> (ls,list,5,3.9)
         * 2> (ss,StringString,2,3.3)
         */

        env.execute("AJoinBExerise Job");
    }

    public static class ABEnrichmentFunction extends RichCoFlatMapFunction<Tuple2<String, String>, Tuple3<String, Integer, Double>, Tuple4<String, String, Integer, Double>> {

        private ValueState<Tuple2State> tuple2ValueState;
        private ValueState<Tuple3State> tuple3ValueState;

        @Override
        public void open(Configuration parameters) throws Exception {
            tuple2ValueState = getRuntimeContext().getState(new ValueStateDescriptor<>("saved a", Tuple2State.class));
            tuple3ValueState = getRuntimeContext().getState(new ValueStateDescriptor<>("saved b", Tuple3State.class));
        }

        // a Tuple2
        @Override
        public void flatMap1(Tuple2<String, String> value, Collector<Tuple4<String, String, Integer, Double>> out) throws Exception {
            // 判断b的状态 并输出
            Tuple3State tuple3State = tuple3ValueState.value();
            if (tuple3State != null && tuple3State.getTuple3() != null) {
                tuple3ValueState.clear();
                out.collect(new Tuple4(value.f0, value.f1, tuple3State.getTuple3().f1, tuple3State.getTuple3().f2));
            } else {
                // 更新a的状态
                tuple2ValueState.update(new Tuple2State().setTuple2(value));
            }
        }

        // b Tuple3
        @Override
        public void flatMap2(Tuple3<String, Integer, Double> value, Collector<Tuple4<String, String, Integer, Double>> out) throws Exception {
            // 判断a的状态 并输出
            Tuple2State tuple2State = tuple2ValueState.value();
            if (tuple2State != null && tuple2State.getTuple2() != null) {
                tuple2ValueState.clear();
                out.collect(new Tuple4(tuple2State.getTuple2().f0, tuple2State.getTuple2().f1, value.f1, value.f2));
            } else {
                // 更新b的状态
                tuple3ValueState.update(new Tuple3State().setTuple3(value));
            }
        }
    }

    private static class Tuple2State implements Serializable {
        private Tuple2<String, String> tuple2;

        public Tuple2<String, String> getTuple2() {
            return tuple2;
        }

        public Tuple2State setTuple2(Tuple2<String, String> tuple2) {
            this.tuple2 = tuple2;
            return this;
        }
    }

    private static class Tuple3State implements Serializable {
        private Tuple3<String, Integer, Double> tuple3;

        public Tuple3<String, Integer, Double> getTuple3() {
            return tuple3;
        }

        public Tuple3State setTuple3(Tuple3<String, Integer, Double> tuple3) {
            this.tuple3 = tuple3;
            return this;
        }
    }

}

/*
 * Copyright 2015 data Artisans GmbH
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.dataartisans.flinktraining.exercises.datastream_java.basics;

import com.dataartisans.flinktraining.exercises.datastream_java.datatypes.TaxiRide;
import com.dataartisans.flinktraining.exercises.datastream_java.sources.TaxiRideSource;
import com.dataartisans.flinktraining.exercises.datastream_java.utils.ExerciseBase;
import com.dataartisans.flinktraining.exercises.datastream_java.utils.GeoUtils;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.elasticsearch2.shaded.org.joda.time.Interval;
import org.apache.flink.streaming.connectors.elasticsearch2.shaded.org.joda.time.Minutes;
import org.apache.flink.util.Collector;

/**
 * The "Ride Cleansing" exercise from the Flink training
 * (http://training.data-artisans.com).
 * The task of the exercise is to filter a data stream of taxi ride records to keep only rides that
 * start and end within New York City. The resulting stream should be printed.
 * <p>
 * Parameters:
 * -input path-to-input-file
 */
public class RideCleansingExercise extends ExerciseBase {
    public static void main(String[] args) throws Exception {

        ParameterTool params = ParameterTool.fromArgs(args);
        final String input = params.get("input", ExerciseBase.pathToRideData);

        final int maxEventDelay = 60;       // events are out of order by max 60 seconds
        final int servingSpeedFactor = 600; // events of 10 minutes are served in 1 second

        // set up streaming execution environment
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        env.setParallelism(ExerciseBase.parallelism);

        // start the data generator
        DataStream<TaxiRide> rides = env.addSource(rideSourceOrTest(new TaxiRideSource(input, maxEventDelay, servingSpeedFactor)));

        DataStream<TaxiRide> filteredRides = rides
                // filter out rides that do not start or stop in NYC
                .filter(new NYCFilter());

        DataStream<EnrichedRide> filteredAndMapedRides = rides
                // filter out rides that do not start or stop in NYC
                .filter(new NYCFilter()).map(new Enrichment());

        DataStream<EnrichedRide> filteredAndFlatMapedRides = rides.
                flatMap(new NYCEnrichment());

        DataStream<EnrichedRide> enrichedNYCRides = rides.
                flatMap(new NYCEnrichment());

        // Integer-> startCell Integer-> duration
        DataStream<Tuple2<Integer, Integer>> minutesByStartCell = enrichedNYCRides
                .flatMap(new FlatMapFunction<EnrichedRide, Tuple2<Integer, Integer>>() {
                    @Override
                    public void flatMap(EnrichedRide ride,
                                        Collector<Tuple2<Integer, Integer>> out) throws Exception {
                        // 出租车出行结束事件
                        if (!ride.isStart) {
                            // 计算从开始到结束的时间间隔
                            Interval rideInterval = new Interval(ride.startTime.toDate().getTime(), ride.endTime.toDate().getTime());
                            Minutes duration = rideInterval.toDuration().toStandardMinutes();
                            out.collect(new Tuple2<>(ride.startCell, duration.getMinutes()));
                        }
                    }
                });

        // fields position
        minutesByStartCell
                // startCell
                .keyBy(0)
                // duration
                .maxBy(1).print();


        // print the filtered stream
//        printOrTest(filteredAndFlatMapedRides);

        // run the cleansing pipeline
        env.execute("Taxi Ride Cleansing");
    }

    private static class NYCFilter implements FilterFunction<TaxiRide> {

        @Override
        public boolean filter(TaxiRide taxiRide) throws Exception {
            // 起点和终点都在纽约
            return GeoUtils.isInNYC(taxiRide.startLon, taxiRide.startLat) && GeoUtils.isInNYC(taxiRide.endLon, taxiRide.endLat);
        }
    }

    /**
     * one-to-one
     */
    public static class Enrichment implements MapFunction<TaxiRide, EnrichedRide> {
        @Override
        public EnrichedRide map(TaxiRide taxiRide) throws Exception {
            return new EnrichedRide(taxiRide);
        }
    }

    /**
     * one-to-more Filter then Map
     */
    public static class NYCEnrichment implements FlatMapFunction<TaxiRide, EnrichedRide> {
        @Override
        public void flatMap(TaxiRide taxiRide, Collector<EnrichedRide> out) throws Exception {
            // 这里举例不太恰当one-to-one 或者 one-to-0 如WordCount里的字符串split是典型的FlatMap
            FilterFunction<TaxiRide> valid = new NYCFilter();
            if (valid.filter(taxiRide)) {
                out.collect(new EnrichedRide(taxiRide));
            }
        }
    }

    /**
     * mapToGridCell
     */
    public static class EnrichedRide extends TaxiRide {
        public int startCell;
        public int endCell;

        public EnrichedRide() {
        }

        public EnrichedRide(TaxiRide ride) {
            this.rideId = ride.rideId;
            this.isStart = ride.isStart;
            this.startTime = ride.startTime;
            this.endTime = ride.endTime;

            this.startCell = GeoUtils.mapToGridCell(ride.startLon, ride.startLat);
            this.endCell = GeoUtils.mapToGridCell(ride.endLon, ride.endLat);
        }

        public String toString() {
            return super.toString() + "," +
                    Integer.toString(this.startCell) + "," +
                    Integer.toString(this.endCell);
        }
    }

}

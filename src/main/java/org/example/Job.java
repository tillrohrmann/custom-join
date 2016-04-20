package org.example;

import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.TypeExtractor;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AssignerWithPunctuatedWatermarks;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.example.join.JoinOperator;

import javax.annotation.Nullable;

/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

public class Job {

	public static void main(String[] args) throws Exception {
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.setParallelism(1);

		env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

		DataStream<Tuple2<Integer, Integer>> input1 = env.fromElements(1,2,3,4,5,6)
			.map(new MapFunction<Integer, Tuple2<Integer, Integer>>() {
				@Override
				public Tuple2<Integer, Integer> map(Integer integer) throws Exception {
					return Tuple2.of(integer % 2, integer);
				}
			}).assignTimestampsAndWatermarks(new AssignerWithPunctuatedWatermarks<Tuple2<Integer, Integer>>() {
				@Nullable
				@Override
				public Watermark checkAndGetNextWatermark(Tuple2<Integer, Integer> integerIntegerTuple2, long l) {
					return new Watermark(l);
				}

				@Override
				public long extractTimestamp(Tuple2<Integer, Integer> integerIntegerTuple2, long l) {
					return integerIntegerTuple2.f1;
				}
			});

		DataStream<Tuple2<Integer, Integer>> input2 = env.fromElements(3,5,6)
			.map(new MapFunction<Integer, Tuple2<Integer, Integer>>() {
				@Override
				public Tuple2<Integer, Integer> map(Integer integer) throws Exception {
					return Tuple2.of(integer % 2, integer);
				}
			}).assignTimestampsAndWatermarks(new AssignerWithPunctuatedWatermarks<Tuple2<Integer, Integer>>() {
				@Nullable
				@Override
				public Watermark checkAndGetNextWatermark(Tuple2<Integer, Integer> integerIntegerTuple2, long l) {
					return new Watermark(l);
				}

				@Override
				public long extractTimestamp(Tuple2<Integer, Integer> integerIntegerTuple2, long l) {
					return integerIntegerTuple2.f1;
				}
			});

		TypeInformation<Tuple2<Integer, Integer>> outTypeInfo = TypeExtractor.getForObject(new Tuple2<>(1,1));
		long windowLength = 3;

		DataStream<Tuple2<Integer, Integer>> result = input1.connect(input2).keyBy(0,0).transform(
			"customJoin",
			outTypeInfo,
			new JoinOperator<>(new JoinFunction<Tuple2<Integer, Integer>, Tuple2<Integer, Integer>, Tuple2<Integer, Integer>>() {
				@Override
				public Tuple2<Integer, Integer> join(Tuple2<Integer, Integer> left, Tuple2<Integer, Integer> right) throws Exception {
					return Tuple2.of(left.f0, left.f1 + right.f1);
				}
			}, windowLength)
		);

		result.print();

		env.execute("Flink Java API Skeleton");
	}
}

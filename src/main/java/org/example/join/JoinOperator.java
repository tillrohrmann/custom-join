package org.example.join;

import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.streaming.api.operators.AbstractUdfStreamOperator;
import org.apache.flink.streaming.api.operators.TwoInputStreamOperator;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;

import java.io.Serializable;
import java.util.Comparator;
import java.util.Iterator;
import java.util.PriorityQueue;

public class JoinOperator<IN1 extends Serializable, IN2 extends Serializable, OUT> extends AbstractUdfStreamOperator<OUT, JoinFunction<IN1, IN2, OUT>>
        implements TwoInputStreamOperator<IN1, IN2, OUT> {

    ValueState<PriorityQueue<StreamRecord<IN1>>> input1Queue;
    ValueState<PriorityQueue<StreamRecord<IN2>>> input2Queue;
    ValueState<Long> watermark1;
    ValueState<Long> watermark2;

    private final long windowLength;

    public JoinOperator(JoinFunction<IN1, IN2, OUT> userFunction, long windowLength) {
        super(userFunction);
        this.windowLength = windowLength;
    }

    @Override
    public void open() throws Exception {
        PriorityQueue<StreamRecord<IN1>> defaultInput1 = new PriorityQueue<>(new StreamRecordComparator<IN1>());
        input1Queue = getPartitionedState(new ValueStateDescriptor<>("input1Queue", (Class<PriorityQueue<StreamRecord<IN1>>>)defaultInput1.getClass(), defaultInput1));

        PriorityQueue<StreamRecord<IN2>> defaultInput2 = new PriorityQueue<>(new StreamRecordComparator<IN2>());
        input2Queue = getPartitionedState(new ValueStateDescriptor<>("input2Queue", (Class<PriorityQueue<StreamRecord<IN2>>>)defaultInput2.getClass(), defaultInput2));

        watermark1 = getPartitionedState(new ValueStateDescriptor<>("watermark1", Long.class, Long.MIN_VALUE));
        watermark2 = getPartitionedState(new ValueStateDescriptor<>("watermark2", Long.class, Long.MIN_VALUE));
    }


    @Override
    public void processElement1(StreamRecord<IN1> streamRecord) throws Exception {
        PriorityQueue<StreamRecord<IN1>> priorityQueue = input1Queue.value();
        priorityQueue.offer(streamRecord);

        input1Queue.update(priorityQueue);
    }

    @Override
    public void processElement2(StreamRecord<IN2> streamRecord) throws Exception {
        PriorityQueue<StreamRecord<IN2>> priorityQueue = input2Queue.value();
        priorityQueue.offer(streamRecord);

        input2Queue.update(priorityQueue);
    }

    @Override
    public void processWatermark1(Watermark watermark) throws Exception {
        watermark1.update(watermark.getTimestamp());

        processWatermark(Math.min(watermark.getTimestamp(), watermark2.value()));
    }

    @Override
    public void processWatermark2(Watermark watermark) throws Exception {
        watermark2.update(watermark.getTimestamp());

        processWatermark(Math.min(watermark1.value(), watermark.getTimestamp()));
    }

    private void processWatermark(long watermark) throws Exception {
        PriorityQueue<StreamRecord<IN1>> input1 = input1Queue.value();
        PriorityQueue<StreamRecord<IN2>> input2 = input2Queue.value();

        while (!input2.isEmpty() && input2.peek().getTimestamp() <= watermark) {
            StreamRecord<IN2> right = input2.poll();
            long lowerWindowBorder = right.getTimestamp() - windowLength;

            Iterator<StreamRecord<IN1>> it = input1.iterator();

            while (it.hasNext()) {
                StreamRecord<IN1> left = it.next();

                if (left.getTimestamp() <= lowerWindowBorder) {
                    it.remove();
                } else {
                    break;
                }
            }

            it = input1.iterator();

            while (it.hasNext()) {
                StreamRecord<IN1> left = it.next();

                if (left.getTimestamp() > right.getTimestamp()) {
                    break;
                } else {
                    OUT result = userFunction.join(left.getValue(), right.getValue());

                    output.collect(new StreamRecord<OUT>(result, right.getTimestamp()));
                }
            }
        }

        long lowerWindowBorder = Math.min(watermark - windowLength, Long.MIN_VALUE);

        while (!input1.isEmpty() && input1.peek().getTimestamp() <= lowerWindowBorder) {
            input1.poll();
        }

        input1Queue.update(input1);
        input2Queue.update(input2);

        output.emitWatermark(new Watermark(watermark));
    }

    public static class StreamRecordComparator<T> implements Comparator<StreamRecord<T>> {

        @Override
        public int compare(StreamRecord<T> o1, StreamRecord<T> o2) {
            if (o1.getTimestamp() == o2.getTimestamp()) {
                return 0;
            } else if (o1.getTimestamp() < o2.getTimestamp()) {
                return -1;
            } else {
                return 1;
            }
        }
    }
}

package org.wso2.siddhi.extension.customWindow;

import org.wso2.siddhi.core.event.stream.StreamEvent;
import org.wso2.siddhi.core.executor.ExpressionExecutor;

import java.util.Comparator;

public class EventList {
    private SortedList<StreamEvent> streamEvents;
    private long punctuation_timestamp;
    private int limitSize;
    private int currentSize;
    private boolean isFilled;

    public EventList(int limitSize, long punctuation_timestamp, final ExpressionExecutor[] attributeExpressionExecutors) {
        this.streamEvents = new SortedList<StreamEvent>(new Comparator<StreamEvent>() {
            @Override
            public int compare(StreamEvent o1, StreamEvent o2) {
                long timestamp1 = (Long) (attributeExpressionExecutors[2].execute(o1));
                long timestamp2 = (Long) (attributeExpressionExecutors[2].execute(o2));

                return (int) (timestamp1 - timestamp2);
            }
        });

        this.currentSize = 0;
        this.limitSize = limitSize;
        this.punctuation_timestamp = punctuation_timestamp;
        this.isFilled = false;
    }

    public boolean add(StreamEvent streamEvent) {
        streamEvents.add(streamEvent);
        currentSize++;

//        System.out.println("currentSize : " + currentSize + " , limitSize : " + limitSize);//TODO : testing
        if (currentSize >= limitSize) {
            isFilled = true;
            return true;
        }
        return false;
    }

    public SortedList<StreamEvent> getStreamEvents() {
        return streamEvents;
    }

    public long getPunctuation_timestamp() {
        return punctuation_timestamp;
    }

    public int getLimitSize() {
        return limitSize;
    }

    public int getCurrentSize() {
        return currentSize;
    }

    public boolean isFilled() {
        return isFilled;
    }
}

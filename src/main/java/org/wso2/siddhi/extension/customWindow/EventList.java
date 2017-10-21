package org.wso2.siddhi.extension.customWindow;

import org.wso2.siddhi.core.event.stream.StreamEvent;
import org.wso2.siddhi.core.executor.ExpressionExecutor;

import java.util.Comparator;

public class EventList {
    private SortedList<StreamEvent> streamEvents;
    private int punctuation_counter;
    private int limitSize;
    private int currentSize;
    private boolean isFilled;

    public EventList(int limitSize, int punctuation_counter, final ExpressionExecutor[] attributeExpressionExecutors) {
        this.streamEvents = new SortedList<StreamEvent>(new Comparator<StreamEvent>() {
            @Override
            public int compare(StreamEvent o1, StreamEvent o2) {
                int counter1 = (Integer) (attributeExpressionExecutors[2].execute(o1));
                int counter2 = (Integer) (attributeExpressionExecutors[2].execute(o2));

                return (counter1 - counter2);
            }
        });

        this.currentSize = 0;
        this.limitSize = limitSize;
        this.punctuation_counter = punctuation_counter;
        if (limitSize == 0) {
            this.isFilled = true;
        } else {
            this.isFilled = false;
        }
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

    public int getPunctuation_counter() {
        return punctuation_counter;
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

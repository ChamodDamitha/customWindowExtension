package org.wso2.siddhi.extension.customWindow;

import org.wso2.siddhi.core.event.stream.StreamEvent;
import org.wso2.siddhi.core.executor.ExpressionExecutor;

import java.util.ArrayList;

public class ListHandler {
    ExpressionExecutor[] attributeExpressionExecutors;

    ArrayList<EventList> eventLists = new ArrayList<EventList>();

    ArrayList<StreamEvent> tempEventList = new ArrayList<StreamEvent>();


    public ListHandler(ExpressionExecutor[] attributeExpressionExecutors) {
        this.attributeExpressionExecutors = attributeExpressionExecutors;
    }


    public synchronized void addNewList(long punctuation_timestamp, int size) {
        eventLists.add(new EventList(size, punctuation_timestamp,attributeExpressionExecutors));
    }

    public synchronized SortedList<StreamEvent> addEvent(StreamEvent streamEvent) {

        tempEventList.add(streamEvent);
//        System.out.println("tempEventList size : " + tempEventList.size());//TODO

        for (int i = 0; i < tempEventList.size(); i++) {
            StreamEvent tempStreamEvent = tempEventList.get(i);
            long event_timestamp = (Long) (attributeExpressionExecutors[2].execute(tempStreamEvent));
            boolean added = false;

            for (EventList eventList : eventLists) {
                if (eventList.getPunctuation_timestamp() >= event_timestamp) {
                    eventList.add(tempStreamEvent);
                    tempEventList.remove(i);
                    i--;
                    added = true;
                    break;
                }
            }

//            if (!added) {
//                System.out.println("not added...............................!");//TODO
//            }

        }

        if (!eventLists.isEmpty()) {
            EventList firstList = eventLists.get(0);
            if (firstList.isFilled()) {
                eventLists.remove(0);
                return firstList.getStreamEvents();
            }
        }
        return null;
    }
}

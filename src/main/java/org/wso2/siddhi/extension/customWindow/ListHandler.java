package org.wso2.siddhi.extension.customWindow;

import org.wso2.siddhi.core.event.stream.StreamEvent;
import org.wso2.siddhi.core.executor.ExpressionExecutor;;

import java.util.ArrayList;
import java.util.Comparator;

public class ListHandler {
    int events = 0;
    private int windowSize;
    ExpressionExecutor[] attributeExpressionExecutors;

    ArrayList<EventList> eventLists = new ArrayList<EventList>();

    SortedList<StreamEvent> tempEventList = new SortedList<StreamEvent>(new Comparator<StreamEvent>() {
        @Override
        public int compare(StreamEvent o1, StreamEvent o2) {
            int counter1 = (Integer) (attributeExpressionExecutors[2].execute(o1));
            int counter2 = (Integer) (attributeExpressionExecutors[2].execute(o2));

            return (int) (counter1 - counter2);
        }
    });


    public ListHandler(int windowSize, ExpressionExecutor[] attributeExpressionExecutors) {
        this.windowSize = windowSize;
        this.attributeExpressionExecutors = attributeExpressionExecutors;
    }


    public synchronized void addNewList(int punctuation_timestamp, int size) {
        eventLists.add(new EventList(size, punctuation_timestamp, attributeExpressionExecutors));
    }

    public synchronized ArrayList<SortedList<StreamEvent>> addEvent(StreamEvent streamEvent) {
        events++;

        tempEventList.add(streamEvent);
//        System.out.println("tempEventList size : " + tempEventList.size());//TODO

//      no punctuations
//        if (eventLists.isEmpty() && tempEventList.size() == windowSize) {
//            SortedList<StreamEvent> returnList = tempEventList;
//            tempEventList = new SortedList<StreamEvent>(new Comparator<StreamEvent>() {
//                @Override
//                public int compare(StreamEvent o1, StreamEvent o2) {
//                    long timestamp1 = (Long) (attributeExpressionExecutors[2].execute(o1));
//                    long timestamp2 = (Long) (attributeExpressionExecutors[2].execute(o2));
//
//                    return (int) (timestamp1 - timestamp2);
//                }
//            });
//            return returnList;
//        }

//      have punctuations
        for (int i = 0; i < tempEventList.size(); i++) {
            StreamEvent tempStreamEvent = tempEventList.get(i);
            int event_counter = (Integer) (attributeExpressionExecutors[2].execute(tempStreamEvent));

            for (EventList eventList : eventLists) {
                if (eventList.getPunctuation_counter() > event_counter) {
                    eventList.add(tempStreamEvent);
                    tempEventList.remove(i);
                    break;
                }
            }
        }

//        if (!eventLists.isEmpty()) {
//            EventList firstList = eventLists.get(0);
//            if (firstList.isFilled()) {
//                eventLists.remove(0);
//                return firstList.getStreamEvents();
//            }
//        }

        ArrayList<SortedList<StreamEvent>> toExpireLists = new ArrayList<SortedList<StreamEvent>>();

        for (int i = 0; i < eventLists.size(); i++) {
            if (eventLists.get(i).isFilled()) {
                toExpireLists.add(eventLists.get(i).getStreamEvents());
                eventLists.remove(i);
                i--;
            } else {
//                System.out.println("i : " + i + ", filled : " + eventLists.get(i).getCurrentSize()
//                        + ", size : " + eventLists.get(i).getLimitSize());//todo
                break;
            }
        }
//        if (toExpireLists.size() > 1) {
//            System.out.println("expire list size : " + toExpireLists.size());//todo
//        }

//        int i =0;
//        for (EventList eventList : eventLists) {
////            if (eventList.isFilled()) {
////                i++;
////            }
//            System.out.println("Size : " + eventList.getCurrentSize() + ", limit : " + eventList.getLimitSize());//todo
//        }
//        System.out.println("Filled : " + i +", all : "+ eventLists.size());//todo
//        System.out.println("events  : " + events);//todo

        return toExpireLists;
//        return null;
    }
}

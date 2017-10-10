package org.wso2.siddhi.extension.customWindow;

import com.mchange.util.AssertException;
import org.wso2.siddhi.core.config.ExecutionPlanContext;
import org.wso2.siddhi.core.event.ComplexEvent;
import org.wso2.siddhi.core.event.ComplexEventChunk;
import org.wso2.siddhi.core.event.state.StateEvent;
import org.wso2.siddhi.core.event.stream.StreamEvent;
import org.wso2.siddhi.core.event.stream.StreamEventCloner;
import org.wso2.siddhi.core.executor.ConstantExpressionExecutor;
import org.wso2.siddhi.core.executor.ExpressionExecutor;
import org.wso2.siddhi.core.executor.VariableExpressionExecutor;
import org.wso2.siddhi.core.query.processor.Processor;
import org.wso2.siddhi.core.query.processor.stream.window.WindowProcessor;
import org.wso2.siddhi.core.query.processor.stream.window.FindableProcessor;
import org.wso2.siddhi.core.table.EventTable;
import org.wso2.siddhi.core.util.collection.operator.Finder;
import org.wso2.siddhi.core.util.collection.operator.MatchingMetaStateHolder;
import org.wso2.siddhi.core.util.parser.OperatorParser;
import org.wso2.siddhi.query.api.exception.ExecutionPlanValidationException;
import org.wso2.siddhi.query.api.expression.Expression;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Custom Sliding Length Window implementation which holds last length events, and gets updated on every event arrival and expiry.
 */
public class CustomWindowExtension extends WindowProcessor implements FindableProcessor {
    private int length;
    private int count = 0;
    private int meta_punctuation;
    private int id;
    private long meta_timestamp;
    private boolean toExpire = false;

    private ArrayList<Long> punctuation_timestamps;

    private ComplexEventChunk<StreamEvent> currentEventChunk = new ComplexEventChunk<StreamEvent>(false);
    private ComplexEventChunk<StreamEvent> expiredEventChunk = null;
    private ExecutionPlanContext executionPlanContext;
    private StreamEvent resetEvent = null;


    /**
     * The init method of the WindowProcessor, this method will be called before other methods
     *
     * @param attributeExpressionExecutors the executors of each function parameters
     * @param executionPlanContext         the context of the execution plan
     */
    @Override
    protected void init(ExpressionExecutor[] attributeExpressionExecutors, ExecutionPlanContext executionPlanContext) {
        this.executionPlanContext = executionPlanContext;
        if (outputExpectsExpiredEvents) {
            expiredEventChunk = new ComplexEventChunk<StreamEvent>(false);
        }
        if (attributeExpressionExecutors.length == 4) {
            length = (Integer) (((ConstantExpressionExecutor) attributeExpressionExecutors[0]).getValue());
        } else {
            throw new ExecutionPlanValidationException("Length batch window should exactly have three parameters" +
                    " (<int> windowLength, <int> punctuation, <long> timestamp, <int> id), but found " + attributeExpressionExecutors.length +
                    " input attributes");
        }

        punctuation_timestamps = new ArrayList<Long>();
    }

    /**
     * The main processing method that will be called upon event arrival
     *
     * @param streamEventChunk  the stream event chunk that need to be processed
     * @param nextProcessor     the next processor to which the success events need to be passed
     * @param streamEventCloner helps to clone the incoming event for local storage or modification
     */
    @Override
    protected synchronized void process(ComplexEventChunk<StreamEvent> streamEventChunk,
                                        Processor nextProcessor, StreamEventCloner streamEventCloner) {
        List<ComplexEventChunk<StreamEvent>> streamEventChunks = new ArrayList<ComplexEventChunk<StreamEvent>>();
        synchronized (this) {
            ComplexEventChunk<StreamEvent> outputStreamEventChunk = new ComplexEventChunk<StreamEvent>(true);
            long currentTime = executionPlanContext.getTimestampGenerator().currentTime();
            while (streamEventChunk.hasNext()) {
                StreamEvent streamEvent = streamEventChunk.next();
                StreamEvent clonedStreamEvent = streamEventCloner.copyStreamEvent(streamEvent);

                meta_punctuation = (Integer) (attributeExpressionExecutors[1].execute(streamEvent));
                meta_timestamp = (Long) (attributeExpressionExecutors[2].execute(streamEvent));
                id = (Integer) (attributeExpressionExecutors[3].execute(streamEvent));

//              Adding the punctuation
                if (meta_punctuation == -1) {
                    punctuation_timestamps.add(meta_timestamp);

//                    System.out.println("punctuation_timestamp : " + meta_timestamp);//TODO : testing
                }

                else if (meta_punctuation != -1) {
                    currentEventChunk.add(clonedStreamEvent);
                    count++;
//                    System.out.println("data_timestamp : " + meta_timestamp + ", id : " + id);//TODO : testing


                    if ((punctuation_timestamps.size() > 0 && meta_timestamp >= punctuation_timestamps.get(0))) {
//                        System.out.println("punctuation_timestamps.get(0) : " + punctuation_timestamps.get(0));//TODO : testing
//                        System.out.println("meta_timestamp : " + meta_timestamp);//TODO : testing
                        punctuation_timestamps.remove(0);
                        toExpire = true;

                    } else if (count == length) {
                        toExpire = true;
                    }
                }






                if (toExpire) {
                    System.out.println("count : " + count);//TODO : testing.....
                    if (outputExpectsExpiredEvents) {
                        if (expiredEventChunk.getFirst() != null) {
                            while (expiredEventChunk.hasNext()) {
                                StreamEvent expiredEvent = expiredEventChunk.next();
                                expiredEvent.setTimestamp(currentTime);
                            }
                            outputStreamEventChunk.add(expiredEventChunk.getFirst());
                        }
                    }
                    if (expiredEventChunk != null) {
                        expiredEventChunk.clear();
                    }

                    if (currentEventChunk.getFirst() != null) {

                        // add reset event in front of current events
                        outputStreamEventChunk.add(resetEvent);
                        resetEvent = null;

                        if (expiredEventChunk != null) {
                            currentEventChunk.reset();
                            while (currentEventChunk.hasNext()) {
                                StreamEvent currentEvent = currentEventChunk.next();
                                StreamEvent toExpireEvent = streamEventCloner.copyStreamEvent(currentEvent);
                                toExpireEvent.setType(StreamEvent.Type.EXPIRED);
                                expiredEventChunk.add(toExpireEvent);
                            }
                        }

                        resetEvent = streamEventCloner.copyStreamEvent(currentEventChunk.getFirst());
                        resetEvent.setType(ComplexEvent.Type.RESET);
                        outputStreamEventChunk.add(currentEventChunk.getFirst());
                    }
                    currentEventChunk.clear();
                    count = 0;
                    if (outputStreamEventChunk.getFirst() != null) {
                        streamEventChunks.add(outputStreamEventChunk);
                    }
                    toExpire = false;
                }
            }
        }
        for (ComplexEventChunk<StreamEvent> outputStreamEventChunk : streamEventChunks) {
            nextProcessor.process(outputStreamEventChunk);
        }
    }

    @Override
    public void start() {
        //Do nothing
    }

    @Override
    public void stop() {
        //Do nothing
    }

    @Override
    public Object[] currentState() {
        if (expiredEventChunk != null) {
            return new Object[]{currentEventChunk.getFirst(), expiredEventChunk.getFirst(), count, resetEvent};
        } else {
            return new Object[]{currentEventChunk.getFirst(), count, resetEvent};
        }
    }

    @Override
    public void restoreState(Object[] state) {
        if (state.length > 3) {
            currentEventChunk.clear();
            currentEventChunk.add((StreamEvent) state[0]);
            expiredEventChunk.clear();
            expiredEventChunk.add((StreamEvent) state[1]);
            count = (Integer) state[2];
            resetEvent = (StreamEvent) state[3];

        } else {
            currentEventChunk.clear();
            currentEventChunk.add((StreamEvent) state[0]);
            count = (Integer) state[1];
            resetEvent = (StreamEvent) state[2];
        }
    }
    @Override
    public synchronized StreamEvent find(StateEvent matchingEvent, Finder finder) {
        return finder.find(matchingEvent, expiredEventChunk, streamEventCloner);
    }

    @Override
    public Finder constructFinder(Expression expression, MatchingMetaStateHolder matchingMetaStateHolder, ExecutionPlanContext executionPlanContext,
                                  List<VariableExpressionExecutor> variableExpressionExecutors, Map<String, EventTable> eventTableMap) {
        if (expiredEventChunk == null) {
            expiredEventChunk = new ComplexEventChunk<StreamEvent>(false);
        }
        return OperatorParser.constructOperator(expiredEventChunk, expression, matchingMetaStateHolder,executionPlanContext,variableExpressionExecutors,eventTableMap);
    }

}

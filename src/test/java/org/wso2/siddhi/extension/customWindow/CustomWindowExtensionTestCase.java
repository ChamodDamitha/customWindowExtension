package org.wso2.siddhi.extension.customWindow;

import org.apache.log4j.Logger;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.wso2.siddhi.core.ExecutionPlanRuntime;
import org.wso2.siddhi.core.SiddhiManager;
import org.wso2.siddhi.core.stream.input.InputHandler;
import org.wso2.siddhi.core.stream.output.StreamCallback;

public class CustomWindowExtensionTestCase {
    static final Logger log = Logger.getLogger(CustomWindowExtension.class);
    private volatile int count;
    private volatile boolean eventArrived;

    @Before
    public void init() {
        count = 0;
        eventArrived = false;
    }

    @Test
    public void CustomWindowTest() throws InterruptedException {
        log.info("Custom Window TestCase");
        SiddhiManager siddhiManager = new SiddhiManager();

//        siddhiManager.setExtension("custom:customWindow", CustomWindowExtension.class);

        String inStreamDefinition = "define stream inputStream (meta_punctuation int, id int);";
        String query = ("@info(name = 'query1') " +
                "from inputStream#window.custom:customWindow(10, meta_punctuation) " +
                "select count(id) as num_count " +
                "insert into outputStream;");

        ExecutionPlanRuntime executionPlanRuntime = siddhiManager.createExecutionPlanRuntime(inStreamDefinition + query);

        executionPlanRuntime.addCallback("outputStream", new StreamCallback() {

            @Override
            public void receive(org.wso2.siddhi.core.event.Event[] events) {
                for (org.wso2.siddhi.core.event.Event event : events) {
                    count++;
//                    System.out.println("Event : " + event.toString());
//                    if (count == 1) {
//                        Assert.assertEquals(1l, event.getData()[0]);
//                    }

                }
            }
        });


        InputHandler inputHandler = executionPlanRuntime.getInputHandler("inputStream");
        executionPlanRuntime.start();

        for (int i = 0; i < 15; i++) {
            inputHandler.send(new Object[]{ i * 10, i});
            Thread.sleep(10);
        }
        executionPlanRuntime.shutdown();

        Thread.sleep(1000);
        Assert.assertEquals(15, count);

    }

//    public static void main(String[] args) {
//        try {
//            new CustomWindowExtensionTestCase().CustomWindowTest();
//        } catch (InterruptedException e) {
//            e.printStackTrace();
//        }
//    }
}
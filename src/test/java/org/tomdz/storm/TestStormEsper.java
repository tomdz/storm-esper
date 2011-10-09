package org.tomdz.storm;

import static backtype.storm.utils.Utils.tuple;
import static com.jayway.awaitility.Awaitility.await;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.testng.Assert.assertEquals;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;
import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.topology.IRichSpout;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;

@Test
public class TestStormEsper
{
    private static class Event
    {
        public final String type;
        public final List<?> data;

        public Event(String type, Object... data)
        {
            this.type = type;
            this.data = Collections.unmodifiableList(Arrays.asList(data));
        }
    }

    private LocalCluster cluster;

    @BeforeMethod(alwaysRun = true)
    public void setUp()
    {
        cluster = new LocalCluster();
    }

    @AfterMethod(alwaysRun = true)
    public void tearDown()
    {
        cluster.shutdown();
    }

    private void runTest(final IRichSpout spout,
                         final EsperBolt bolt,
                         final Event... expectedData) throws Exception
    {
        final GatheringBolt gatheringBolt = new GatheringBolt();
        final TopologyBuilder builder = new TopologyBuilder();

        builder.setSpout(1, spout);
        builder.setBolt(2, bolt).shuffleGrouping(1);

        int boltId = 3; 
        for (String eventType : bolt.getEventTypes()) {
            builder.setBolt(boltId++, gatheringBolt).shuffleGrouping(2, bolt.getStreamIdForEventType(eventType));
        }

        Config conf = new Config();
        conf.setDebug(true);

        cluster.submitTopology("test", conf, builder.createTopology());

        await().atMost(10, SECONDS).until(new Callable<Boolean>() {
            @Override
            public Boolean call() throws Exception {
                return gatheringBolt.getGatheredData().size() == expectedData.length;
            }
        });

        Map<String, List<List<?>>> expected = new HashMap<String, List<List<?>>>();
        Map<String, List<List<?>>> actual = new HashMap<String, List<List<?>>>();

        for (Event event : expectedData) {
            List<List<?>> expectedForType = expected.get(event.type);
            if (expectedForType == null) {
                expectedForType = new ArrayList<List<?>>();
                expected.put(event.type, expectedForType);
            }
            expectedForType.add(event.data);
        }
        for (Tuple tuple : gatheringBolt.getGatheredData()) {
            int streamId = tuple.getSourceStreamId();
            String eventType = bolt.getEventTypeForStreamId(streamId);
            Fields fields = bolt.getFieldsForEventType(eventType);

            assertEquals(tuple.getFields().toList(), fields.toList());

            List<List<?>> actualForType = actual.get(eventType);
            if (actualForType == null) {
                actualForType = new ArrayList<List<?>>();
                actual.put(eventType, actualForType);
            }
            actualForType.add(tuple.getValues());
        }
        assertEquals(actual, expected);
    }

    public void testSimple() throws Exception
    {
        @SuppressWarnings("unchecked")
        TestSpout spout = new TestSpout(new Fields("a", "b"), tuple(4, 1), tuple(2, 3), tuple(1, 2), tuple(3, 4));
        EsperBolt esperBolt = new EsperBolt.Builder()
                                           .addInputAlias(1, 1, "Test")
                                           .setAnonymousOutput(1, "min", "max")
                                           .addStatement("select max(a) as max, min(b) as min from Test.win:length_batch(4)")
                                           .build();

        runTest(spout, esperBolt, new Event(esperBolt.getEventTypeForStreamId(1), 1, 4));
    }

    public void testMultipleStatements() throws Exception
    {
        @SuppressWarnings("unchecked")
        TestSpout spout = new TestSpout(new Fields("a", "b"), tuple(4, 1), tuple(2, 3), tuple(1, 2), tuple(3, 4));
        EsperBolt esperBolt = new EsperBolt.Builder()
                                           .addInputAlias(1, 1, "Test")
                                           .addNamedOutput(1, "MaxValue", "max")
                                           .addNamedOutput(2, "MinValue", "min")
                                           .addStatement("insert into MaxValue select max(a) as max from Test.win:length_batch(4)")
                                           .addStatement("insert into MinValue select min(b) as min from Test.win:length_batch(4)")
                                           .build();

        runTest(spout, esperBolt, new Event("MaxValue", 4), new Event("MinValue", 1));
    }
}

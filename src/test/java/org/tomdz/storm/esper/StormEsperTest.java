package org.tomdz.storm.esper;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.generated.StormTopology;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.io.IOException;
import java.net.ServerSocket;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.Callable;

import static backtype.storm.utils.Utils.tuple;
import static com.jayway.awaitility.Awaitility.await;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.testng.Assert.assertEquals;

@Test
public class StormEsperTest
{
    private static final String GATHERER = "gatherer";
    private LocalCluster cluster;

    @BeforeMethod(alwaysRun = true)
    public void setUp()
    {
        cluster = new LocalCluster();
    }

    @AfterMethod(alwaysRun = true)
    public void tearDown() throws Exception
    {
        cluster.killTopology("test");
        cluster.shutdown();
    }

    private int getFreePort() throws IOException
    {
        ServerSocket socket = new ServerSocket(0);

        int port = socket.getLocalPort();

        socket.close();

        return port;
    }
    
    private void runTest(final TestTopologyBuilder topologyBuilder,
                         final Event... expectedData) throws Exception
    {
        final StormTopology topology = topologyBuilder.build();
        final GatheringBolt gatheringBolt = (GatheringBolt)topologyBuilder.getBolt(GATHERER);
        final Config conf = new Config();

        conf.setDebug(true);
        conf.put(Config.STORM_ZOOKEEPER_PORT, getFreePort());
        conf.put(Config.NIMBUS_THRIFT_PORT, getFreePort());

        cluster.submitTopology("test", conf, topology);

        await().atMost(10, SECONDS).until(new Callable<Boolean>() {
            @Override
            public Boolean call() throws Exception {
            return gatheringBolt.getGatheredData().size() == expectedData.length;
            }
        });

        Set<Event> actual = new HashSet<Event>();
        Set<Event> expected = new HashSet<Event>(Arrays.asList(expectedData));

        for (Tuple tuple : gatheringBolt.getGatheredData()) {
            String componentId = tuple.getSourceComponent();
            String streamId = tuple.getSourceStreamId();
            EsperBolt bolt = (EsperBolt)topologyBuilder.getBolt(componentId);
            String eventType = bolt.getEventTypeForStreamId(streamId);
            Fields fields = bolt.getFieldsForEventType(eventType);

            assertEquals(new HashSet<String>(tuple.getFields().toList()),
                         new HashSet<String>(fields.toList()));

            actual.add(new Event(componentId, streamId, eventType, tuple.getValues().toArray()));
        }
        assertEquals(actual, expected);
    }

    @SuppressWarnings("unchecked")
    public void testSimple() throws Exception
    {
        TestSpout spout = new TestSpout(new Fields("a", "b"), tuple(4, 1), tuple(2, 3), tuple(1, 2), tuple(3, 4));
        EsperBolt esperBolt = new EsperBolt.Builder()
                                           .addInputAlias("spout1", "Test")
                                           .setAnonymousOutput("default", "max", "sum")
                                           .addStatement("select max(a) as max, sum(b) as sum from Test.win:length_batch(4)")
                                           .build();

        runTest(new TestTopologyBuilder().addSpout("spout1", spout)
                                         .addBolt("bolt1", esperBolt)
                                         .addBolt(GATHERER, new GatheringBolt())
                                         .connect("spout1", "bolt1")
                                         .connect("bolt1", GATHERER),
                new Event("bolt1", "default", null, 4, 10));
    }

    @SuppressWarnings("unchecked")
    public void testMultipleStatements() throws Exception
    {
        TestSpout spout = new TestSpout(new Fields("a", "b"), tuple(4, 1), tuple(2, 3), tuple(1, 2), tuple(3, 4));
        EsperBolt esperBolt = new EsperBolt.Builder()
                                           .addInputAlias("spout1", "Test")
                                           .addNamedOutput("stream1", "MaxValue", "max")
                                           .addNamedOutput("stream2", "MinValue", "min")
                                           .addStatement("insert into MaxValue select max(a) as max from Test.win:length_batch(4)")
                                           .addStatement("insert into MinValue select min(b) as min from Test.win:length_batch(4)")
                                           .build();

        runTest(new TestTopologyBuilder().addSpout("spout1", spout)
                                         .addBolt("bolt1", esperBolt)
                                         .addBolt(GATHERER, new GatheringBolt())
                                         .connect("spout1", "bolt1")
                                         .connect("bolt1", "stream1", GATHERER)
                                         .connect("bolt1", "stream2", GATHERER),
                new Event("bolt1", "stream1", "MaxValue", 4),
                new Event("bolt1", "stream2", "MinValue", 1));
    }

    @SuppressWarnings("unchecked")
    public void testMultipleSpouts() throws Exception
    {
        TestSpout spout1 = new TestSpout(new Fields("a"), tuple(4), tuple(2), tuple(1), tuple(3));
        TestSpout spout2 = new TestSpout(new Fields("b"), tuple(1), tuple(3), tuple(2), tuple(4));
        EsperBolt esperBolt = new EsperBolt.Builder()
                                           .addInputAlias("spout1", "TestA")
                                           .addInputAlias("spout2", "TestB")
                                           .setAnonymousOutput("default", "min", "max")
                                           .addStatement("select max(a) as max, min(b) as min from TestA.win:length_batch(4), TestB.win:length_batch(4)")
                                           .build();

        runTest(new TestTopologyBuilder().addSpout("spout1", spout1)
                                         .addSpout("spout2", spout2)
                                         .addBolt("bolt1", esperBolt)
                                         .addBolt(GATHERER, new GatheringBolt())
                                         .connect("spout1", "bolt1")
                                         .connect("spout2", "bolt1")
                                         .connect("bolt1", GATHERER),
                new Event("bolt1", "default", null, 1, 4));
    }

    @SuppressWarnings("unchecked")
    public void testNoInputAlias() throws Exception
    {
        TestSpout spout = new TestSpout(new Fields("a", "b"), tuple(4, 1), tuple(2, 3), tuple(1, 2), tuple(3, 4));
        EsperBolt esperBolt = new EsperBolt.Builder()
                                           .setAnonymousOutput("default", "min", "max")
                                           .addStatement("select max(a) as max, min(b) as min from spout1_default.win:length_batch(4)")
                                           .build();

        runTest(new TestTopologyBuilder().addSpout("spout1", spout)
                                         .addBolt("bolt1", esperBolt)
                                         .addBolt(GATHERER, new GatheringBolt())
                                         .connect("spout1", "bolt1")
                                         .connect("bolt1", GATHERER),
                new Event("bolt1", "default", null, 1, 4));
    }

    @SuppressWarnings("unchecked")
    public void testMultipleSpoutsWithoutInputAlias() throws Exception
    {
        TestSpout spout1 = new TestSpout(new Fields("a"), tuple(4), tuple(2), tuple(1), tuple(3));
        TestSpout spout2 = new TestSpout(new Fields("b"), tuple(1), tuple(3), tuple(2), tuple(4));
        EsperBolt esperBolt = new EsperBolt.Builder()
                                           .addInputAlias("spout1", "default", "TestA")
                                           .setAnonymousOutput("default", "min", "max")
                                           .addStatement("select max(a) as max, min(b) as min from TestA.win:length_batch(4), spout2_default.win:length_batch(4)")
                                           .build();

        runTest(new TestTopologyBuilder().addSpout("spout1", spout1)
                                         .addSpout("spout2", spout2)
                                         .addBolt("bolt1", esperBolt)
                                         .addBolt(GATHERER, new GatheringBolt())
                                         .connect("spout1", "bolt1")
                                         .connect("spout2", "bolt1")
                                         .connect("bolt1", GATHERER),
                new Event("bolt1", "default", null, 1, 4));
    }

    @SuppressWarnings("unchecked")
    public void testMultipleBolts() throws Exception
    {
        TestSpout spout1 = new TestSpout(new Fields("a"), tuple(4), tuple(2), tuple(1), tuple(3));
        TestSpout spout2 = new TestSpout(new Fields("b"), tuple(1), tuple(3), tuple(2), tuple(4));
        EsperBolt esperBolt1 = new EsperBolt.Builder()
                                            .addInputAlias("spout1", "Test")
                                            .setAnonymousOutput("default", "max")
                                            .addStatement("select max(a) as max from Test.win:length_batch(4)")
                                            .build();
        EsperBolt esperBolt2 = new EsperBolt.Builder()
                                            .addInputAlias("spout2", "Test")
                                            .setAnonymousOutput("default", "min")
                                            .addStatement("select min(b) as min from Test.win:length_batch(4)")
                                            .build();

        runTest(new TestTopologyBuilder().addSpout("spout1", spout1)
                                         .addSpout("spout2", spout2)
                                         .addBolt("bolt1", esperBolt1)
                                         .addBolt("bolt2", esperBolt2)
                                         .addBolt(GATHERER, new GatheringBolt())
                                         .connect("spout1", "bolt1")
                                         .connect("spout2", "bolt2")
                                         .connect("bolt1", GATHERER)
                                         .connect("bolt2", GATHERER),
                new Event("bolt1", "default", null, 4),
                new Event("bolt2", "default", null, 1));
    }

    // Can't test this yet because we can't catch the error ?
    @Test(enabled = false)
    public void testNoSuchSpout() throws Exception
    {
        EsperBolt esperBolt = new EsperBolt.Builder()
                                           .setAnonymousOutput("default", "min", "max")
                                           .addStatement("select max(a) as max, min(b) as min from spout1_default.win:length_batch(4)")
                                           .build();

        runTest(new TestTopologyBuilder().addBolt("bolt1", esperBolt)
                                         .addBolt(GATHERER, new GatheringBolt())
                                         .connect("spout1", "bolt1")
                                         .connect("bolt1", GATHERER));
    }
    // TODO: more tests
    // adding alias for undefined spout
    // using the same alias twice
    // using same stream id twice for named output
    // multiple esper bolts
}

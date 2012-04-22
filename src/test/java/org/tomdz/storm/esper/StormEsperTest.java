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
    private String topologyName;

    @BeforeMethod(alwaysRun = true)
    public void setUp()
    {
        cluster = new LocalCluster();
        topologyName = "test" + System.currentTimeMillis();
    }

    @AfterMethod(alwaysRun = true)
    public void tearDown() throws Exception
    {
        cluster.killTopology(topologyName);
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

        cluster.submitTopology(topologyName, conf, topology);

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

            // we'll ignore data from other bolts (such as bolts from previous tests
            // which for some reason are still around)
            if (bolt == null) {
                System.err.println("Didn't find bolt for " + componentId + ", " + streamId);
            }
            else {
                EventTypeDescriptor eventType = bolt.getEventTypeForStreamId(streamId);

                assertEquals(new HashSet<String>(tuple.getFields().toList()),
                             new HashSet<String>(eventType.getFields().toList()));

                actual.add(new Event(componentId, streamId, eventType.getName(), tuple.getValues().toArray()));
            }
        }
        assertEquals(actual, expected);
    }

    @SuppressWarnings("unchecked")
    public void testSimple() throws Exception
    {
        TestSpout spout = new TestSpout(new Fields("a", "b"), tuple(4, 1), tuple(2, 3), tuple(1, 2), tuple(3, 4));
        EsperBolt esperBolt = new EsperBolt.Builder()
                                           .inputs().aliasComponent("spout1A").withFields("a", "b").ofType(Integer.class).toEventType("Test1A")
                                           .outputs().onDefaultStream().emit("max", "sum")
                                           .statements().add("select max(a) as max, sum(b) as sum from Test1A.win:length_batch(4)")
                                           .build();

        runTest(new TestTopologyBuilder().addSpout("spout1A", spout)
                                         .addBolt("bolt1A", esperBolt)
                                         .addBolt(GATHERER, new GatheringBolt())
                                         .connect("spout1A", "bolt1A")
                                         .connect("bolt1A", GATHERER),
                new Event("bolt1A", "default", null, 4, 10));
    }

    @SuppressWarnings("unchecked")
    public void testMultipleStatements() throws Exception
    {
        TestSpout spout = new TestSpout(new Fields("a", "b"), tuple(4, 1), tuple(2, 3), tuple(1, 2), tuple(3, 4));
        EsperBolt esperBolt = new EsperBolt.Builder()
                                           .inputs().aliasComponent("spout2A").toEventType("Test2A")
                                           .outputs().onStream("stream1").fromEventType("MaxValue").emit("max")
                                                     .onStream("stream2").fromEventType("MinValue").emit("min")
                                           .statements().add("insert into MaxValue select max(a) as max from Test2A.win:length_batch(4)")
                                                        .add("insert into MinValue select min(b) as min from Test2A.win:length_batch(4)")
                                           .build();

        runTest(new TestTopologyBuilder().addSpout("spout2A", spout)
                                         .addBolt("bolt2A", esperBolt)
                                         .addBolt(GATHERER, new GatheringBolt())
                                         .connect("spout2A", "bolt2A")
                                         .connect("bolt2A", "stream1", GATHERER)
                                         .connect("bolt2A", "stream2", GATHERER),
                new Event("bolt2A", "stream1", "MaxValue", 4),
                new Event("bolt2A", "stream2", "MinValue", 1));
    }

    @SuppressWarnings("unchecked")
    public void testMultipleSpouts() throws Exception
    {
        TestSpout spout1 = new TestSpout(new Fields("a"), tuple(4), tuple(2), tuple(1), tuple(3));
        TestSpout spout2 = new TestSpout(new Fields("b"), tuple(1), tuple(3), tuple(2), tuple(4));
        EsperBolt esperBolt = new EsperBolt.Builder()
                                           .inputs().aliasComponent("spout3A").toEventType("Test3A")
                                                    .aliasComponent("spout3B").toEventType("Test3B")
                                           .outputs().onDefaultStream().emit("min", "max")
                                           .statements().add("select max(a) as max, min(b) as min from Test3A.win:length_batch(4), Test3B.win:length_batch(4)")
                                           .build();

        runTest(new TestTopologyBuilder().addSpout("spout3A", spout1)
                                         .addSpout("spout3B", spout2)
                                         .addBolt("bolt3A", esperBolt)
                                         .addBolt(GATHERER, new GatheringBolt())
                                         .connect("spout3A", "bolt3A")
                                         .connect("spout3B", "bolt3A")
                                         .connect("bolt3A", GATHERER),
                new Event("bolt3A", "default", null, 1, 4));
    }

    @SuppressWarnings("unchecked")
    public void testNoInputAlias() throws Exception
    {
        TestSpout spout = new TestSpout(new Fields("a", "b"), tuple(4, 1), tuple(2, 3), tuple(1, 2), tuple(3, 4));
        EsperBolt esperBolt = new EsperBolt.Builder()
                                           .outputs().onDefaultStream().emit("min", "max")
                                           .statements().add("select max(a) as max, min(b) as min from spout4A_default.win:length_batch(4)")
                                           .build();

        runTest(new TestTopologyBuilder().addSpout("spout4A", spout)
                                         .addBolt("bolt4A", esperBolt)
                                         .addBolt(GATHERER, new GatheringBolt())
                                         .connect("spout4A", "bolt4A")
                                         .connect("bolt4A", GATHERER),
                new Event("bolt4A", "default", null, 1, 4));
    }

    @SuppressWarnings("unchecked")
    public void testMultipleSpoutsWithoutInputAlias() throws Exception
    {
        TestSpout spout1 = new TestSpout(new Fields("a"), tuple(4), tuple(2), tuple(1), tuple(3));
        TestSpout spout2 = new TestSpout(new Fields("b"), tuple(1), tuple(3), tuple(2), tuple(4));
        EsperBolt esperBolt = new EsperBolt.Builder()
                                           .inputs().aliasStream("spout5A", "default").toEventType("Test5A")
                                           .outputs().onDefaultStream().emit("min", "max")
                                           .statements().add("select max(a) as max, min(b) as min from Test5A.win:length_batch(4), spout5B_default.win:length_batch(4)")
                                           .build();

        runTest(new TestTopologyBuilder().addSpout("spout5A", spout1)
                                         .addSpout("spout5B", spout2)
                                         .addBolt("bolt5A", esperBolt)
                                         .addBolt(GATHERER, new GatheringBolt())
                                         .connect("spout5A", "bolt5A")
                                         .connect("spout5B", "bolt5A")
                                         .connect("bolt5A", GATHERER),
                new Event("bolt5A", "default", null, 1, 4));
    }

    @SuppressWarnings("unchecked")
    public void testMultipleBolts() throws Exception
    {
        TestSpout spout1 = new TestSpout(new Fields("a"), tuple(4), tuple(2), tuple(1), tuple(3));
        TestSpout spout2 = new TestSpout(new Fields("b"), tuple(1), tuple(3), tuple(2), tuple(4));
        EsperBolt esperBolt1 = new EsperBolt.Builder()
                                            .inputs().aliasComponent("spout6A").toEventType("Test6A")
                                            .outputs().onDefaultStream().emit("max")
                                            .statements().add("select max(a) as max from Test6A.win:length_batch(4)")
                                            .build();
        EsperBolt esperBolt2 = new EsperBolt.Builder()
                                            .inputs().aliasComponent("spout6B").toEventType("Test6B")
                                            .outputs().onDefaultStream().emit("min")
                                            .statements().add("select min(b) as min from Test6B.win:length_batch(4)")
                                            .build();

        runTest(new TestTopologyBuilder().addSpout("spout6A", spout1)
                                         .addSpout("spout6B", spout2)
                                         .addBolt("bolt6A", esperBolt1)
                                         .addBolt("bolt6B", esperBolt2)
                                         .addBolt(GATHERER, new GatheringBolt())
                                         .connect("spout6A", "bolt6A")
                                         .connect("spout6B", "bolt6B")
                                         .connect("bolt6A", GATHERER)
                                         .connect("bolt6B", GATHERER),
                new Event("bolt6A", "default", null, 4),
                new Event("bolt6B", "default", null, 1));
    }

    // Can't test this yet because we can't catch the error ?
    @Test(enabled = false)
    public void testNoSuchSpout() throws Exception
    {
        EsperBolt esperBolt = new EsperBolt.Builder()
                                           .outputs().onDefaultStream().emit("min", "max")
                                           .statements().add("select max(a) as max, min(b) as min from spout7A_default.win:length_batch(4)")
                                           .build();

        runTest(new TestTopologyBuilder().addBolt("bolt7A", esperBolt)
                                         .addBolt(GATHERER, new GatheringBolt())
                                         .connect("spout7A", "bolt7A")
                                         .connect("bolt7A", GATHERER));
    }
    // TODO: more tests
    // adding aliasComponent for undefined spout
    // using the same aliasComponent twice
    // using same stream id twice for named output
    // multiple esper bolts
}

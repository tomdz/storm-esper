package org.tomdz.storm;

import static backtype.storm.utils.Utils.tuple;
import static com.jayway.awaitility.Awaitility.await;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.testng.Assert.assertEquals;
import java.util.Arrays;
import java.util.concurrent.Callable;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;
import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;

@Test
public class TestStormEsper
{
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

    public void testSimple() throws Exception
    {
        @SuppressWarnings("unchecked")
        final TestSpout spout = new TestSpout(new Fields("a", "b"), tuple(4, 1), tuple(2, 3), tuple(1, 2), tuple(3, 4));
        final EsperBolt esperBolt = new EsperBolt.Builder()
                                                 .addInputAlias(1, 1, "Test")
                                                 .setAnonymousOutput(1, "min", "max")
                                                 .addStatement("select max(a) as max, min(b) as min from Test.win:length_batch(4)")
                                                 .build();
        final GatheringBolt gatheringBolt = new GatheringBolt();
        final TopologyBuilder builder = new TopologyBuilder();

        builder.setSpout(1, spout);
        builder.setBolt(2, esperBolt).shuffleGrouping(1);
        builder.setBolt(3, gatheringBolt).shuffleGrouping(2);

        Config conf = new Config();
        conf.setDebug(true);

        cluster.submitTopology("test", conf, builder.createTopology());

        await().atMost(10, SECONDS).until(new Callable<Boolean>() {
            @Override
            public Boolean call() throws Exception {
                return gatheringBolt.getGatheredData().size() == 1;
            }
        });

        Tuple tuple = gatheringBolt.getGatheredData().get(0);

        assertEquals(tuple.getFields().size(), 2);
        assertEquals(tuple.getFields().get(0), "min");
        assertEquals(tuple.getFields().get(1), "max");
        assertEquals(tuple.getValues(), Arrays.asList(1, 4));
    }
}

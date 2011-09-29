package org.tomdz.storm;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;
import backtype.storm.utils.Utils;

public class EsperTest
{
    public static void main(String[] args)
    {
        final String username = args[0];
        final String pwd = args[1];

        TopologyBuilder builder = new TopologyBuilder();
        TwitterSpout spout = new TwitterSpout(username, pwd);
        EsperBolt bolt = new EsperBolt(new Fields("tps", "maxRetweets"),
                                       "select count(*) as tps, max(retweetCount) as maxRetweets from Storm.win:time_batch(1 sec)");

        builder.setSpout(1, spout);
        builder.setBolt(2, bolt).shuffleGrouping(1);

        Config conf = new Config();
        conf.setDebug(true);

        LocalCluster cluster = new LocalCluster();

        cluster.submitTopology("test", conf, builder.createTopology());
        Utils.sleep(10000);
        cluster.shutdown();
    }
}

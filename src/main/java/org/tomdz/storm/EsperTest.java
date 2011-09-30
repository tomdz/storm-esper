package org.tomdz.storm;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.utils.Utils;

public class EsperTest
{
    public static void main(String[] args)
    {
        final String username = args[0];
        final String pwd = args[1];

        TopologyBuilder builder = new TopologyBuilder();
        TwitterSpout spout = new TwitterSpout(username, pwd);
        EsperBolt bolt = new EsperBolt.Builder()
                                      .addInputAlias(1, 1, "Tweets")
                                      .setAnonymousOutput("tps", "maxRetweets")
                                      .addStatement("select count(*) as tps, max(retweetCount) as maxRetweets from Tweets.win:time_batch(1 sec)")
                                      .build();

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

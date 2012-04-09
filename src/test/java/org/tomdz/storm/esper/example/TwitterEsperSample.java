package org.tomdz.storm.esper.example;

import org.tomdz.storm.esper.EsperBolt;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.utils.Utils;

public class TwitterEsperSample
{
    public static void main(String[] args)
    {
        final String username = args[0];
        final String pwd = args[1];

        TopologyBuilder builder = new TopologyBuilder();
        TwitterSpout spout = new TwitterSpout(username, pwd);
        EsperBolt bolt = new EsperBolt.Builder()
                                      .addInputAlias("spout1", "default", "Tweets")
                                      .setAnonymousOutput("default", "tps", "maxRetweets")
                                      .addStatement("select count(*) as tps, max(retweetCount) as maxRetweets from Tweets.win:time_batch(1 sec)")
                                      .build();

        builder.setSpout("spout1", spout);
        builder.setBolt("bolt1", bolt).shuffleGrouping("spout1");

        Config conf = new Config();
        conf.setDebug(true);

        LocalCluster cluster = new LocalCluster();

        cluster.submitTopology("test", conf, builder.createTopology());
        Utils.sleep(10000);
        cluster.shutdown();
    }
}

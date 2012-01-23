package org.tomdz.storm.esper;

import java.util.List;

import org.tomdz.storm.esper.EsperBolt;

import backtype.storm.generated.StormTopology;
import backtype.storm.topology.InputDeclarer;
import backtype.storm.topology.TopologyBuilder;

public class TestTopologyBuilder
{
    private TopologyBuilder builder = new TopologyBuilder();
    private int counter = 1;

    public TestTopologyBuilder addSpouts(List<TestSpout> spouts)
    {
        for (TestSpout spout : spouts) {
            builder.setSpout("spout" + counter++, spout);
        }
        return this;
    }

    public TestTopologyBuilder setBolts(EsperBolt esperBolt, GatheringBolt gatheringBolt)
    {
        int esperBoltId = counter++;
        InputDeclarer declarer = builder.setBolt("bolt" + esperBoltId, esperBolt);
        for (int id = 1; id < esperBoltId; id++) {
            declarer = declarer.shuffleGrouping("spout" + id);
        }
        for (String eventType : esperBolt.getEventTypes()) {
            builder.setBolt("bolt" + counter++, gatheringBolt)
                   .shuffleGrouping("bolt" + esperBoltId, esperBolt.getStreamIdForEventType(eventType));
        }
        return this;
    }

    public StormTopology build()
    {
        return builder.createTopology();
    }
}
package org.tomdz.storm.esper;

import java.lang.String;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import backtype.storm.spout.ISpout;
import backtype.storm.task.IBolt;
import backtype.storm.topology.BoltDeclarer;

import backtype.storm.generated.StormTopology;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.IRichSpout;
import backtype.storm.topology.InputDeclarer;
import backtype.storm.topology.TopologyBuilder;

public class TestTopologyBuilder
{
    private final Map<String, IRichSpout> spoutMap = new HashMap<String, IRichSpout>();
    private final Map<String, IRichBolt> boltMap = new HashMap<String, IRichBolt>();
    private final Map<String, List<Connection>> connections = new HashMap<String, List<Connection>>();

    public TestTopologyBuilder addSpout(String name, IRichSpout spout)
    {
        spoutMap.put(name, spout);
        return this;
    }

    public TestTopologyBuilder addBolt(String name, IRichBolt bolt)
    {
        boltMap.put(name, bolt);
        return this;
    }

    public IRichBolt getBolt(String name)
    {
        return boltMap.get(name);
    }

    public TestTopologyBuilder connect(String sourceComponent, String targetComponent)
    {
        return connect(sourceComponent, "default", targetComponent);
    }

    public TestTopologyBuilder connect(String sourceComponent, String sourceStream, String targetComponent)
    {
        List<Connection> connectionsForTarget = connections.get(targetComponent);
        if (connectionsForTarget == null) {
            connectionsForTarget = new ArrayList<Connection>();
            connections.put(targetComponent, connectionsForTarget);
        }
        connectionsForTarget.add(new Connection(sourceComponent, sourceStream, targetComponent));
        return this;
    }

    public StormTopology build()
    {
        final TopologyBuilder builder = new TopologyBuilder();

        for (Map.Entry<String, IRichSpout> spoutEntry : spoutMap.entrySet()) {
            builder.setSpout(spoutEntry.getKey(), spoutEntry.getValue());
        }
        for (Map.Entry<String, IRichBolt> boltEntry : boltMap.entrySet()) {
            InputDeclarer declarer = builder.setBolt(boltEntry.getKey(), boltEntry.getValue());
            List<Connection> connectionsForTarget = connections.get(boltEntry.getKey());
            if (connectionsForTarget != null) {
                for (Connection connection : connectionsForTarget) {
                    declarer = declarer.shuffleGrouping(connection.getSourceComponent(), connection.getSourceStream());
                }
            }
        }
        return builder.createTopology();
    }
}
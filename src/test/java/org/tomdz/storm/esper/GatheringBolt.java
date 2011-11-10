package org.tomdz.storm.esper;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Tuple;

public class GatheringBolt implements IRichBolt
{
    private static final long serialVersionUID = 1L;

    private static final List<Tuple> tuples = new CopyOnWriteArrayList<Tuple>();

    @Override
    public void prepare(@SuppressWarnings("rawtypes") Map stormConf,
                        TopologyContext context,
                        OutputCollector collector)
    {
        tuples.clear();
    }

    @Override
    public void execute(Tuple input)
    {
        tuples.add(input.copyWithNewId(0));
    }

    public List<Tuple> getGatheredData()
    {
        return new ArrayList<Tuple>(tuples);
    }

    @Override
    public void cleanup() {}

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {}
}

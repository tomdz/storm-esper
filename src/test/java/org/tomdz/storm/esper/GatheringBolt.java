package org.tomdz.storm.esper;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.TupleImpl;

public class GatheringBolt extends BaseRichBolt
{
    private static final long serialVersionUID = 1L;

    private static final List<Tuple> tuples = new CopyOnWriteArrayList<Tuple>();
    private transient TopologyContext context;

    @Override
    public void prepare(@SuppressWarnings("rawtypes") Map stormConf,
                        TopologyContext context,
                        OutputCollector collector)
    {
        this.context = context;
        tuples.clear();
    }

    @Override
    public void execute(Tuple input)
    {
        Tuple newTuple = new TupleImpl(context, input.getValues(), input.getSourceTask(), input.getSourceStreamId());

        tuples.add(newTuple);
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

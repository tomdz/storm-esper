package org.tomdz.storm.esper;

import java.util.List;
import java.util.Map;
import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;

public class TestSpout extends BaseRichSpout
{
    private static final long serialVersionUID = 1L;

    private final Fields fields;
    private final List<Object>[] data;
    private transient int curIdx;
    private transient SpoutOutputCollector collector;

    public TestSpout(Fields fields, List<Object>... data)
    {
        this.fields = fields;
        this.data = data;
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer)
    {
        declarer.declare(fields);
    }

    @Override
    public void nextTuple()
    {
        if (curIdx < data.length) {
            collector.emit(data[curIdx++]);
        }
    }

    @Override
    public void open(@SuppressWarnings("rawtypes") Map conf,
                     TopologyContext context,
                     SpoutOutputCollector collector)
    {
        this.collector = collector;
    }

    @Override
    public void close() {}

    @Override
    public void ack(Object msgId) {}

    @Override
    public void fail(Object msgId) {}
}

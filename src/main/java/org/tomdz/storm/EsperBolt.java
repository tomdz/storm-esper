package org.tomdz.storm;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import backtype.storm.generated.GlobalStreamId;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;

import com.espertech.esper.client.Configuration;
import com.espertech.esper.client.EPAdministrator;
import com.espertech.esper.client.EPRuntime;
import com.espertech.esper.client.EPServiceProvider;
import com.espertech.esper.client.EPServiceProviderManager;
import com.espertech.esper.client.EPStatement;
import com.espertech.esper.client.EventBean;
import com.espertech.esper.client.UpdateListener;

public class EsperBolt implements IRichBolt, UpdateListener
{
    private static final long serialVersionUID = 1L;

    private final Fields outputFields;
    private final String[] statements;
    private transient EPRuntime runtime;
    private transient EPAdministrator admin;
    private transient OutputCollector collector;
    private transient boolean singleEventType;

    public EsperBolt(Fields outputFields, String... esperStmts)
    {
        this.outputFields = outputFields;
        this.statements = esperStmts;
    }
    
    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer)
    {
        declarer.declare(outputFields);
    }

    @Override
    public void prepare(Map conf, TopologyContext context, OutputCollector collector)
    {
        this.collector = collector;

        Configuration configuration = new Configuration();

        setupEventTypes(context, configuration);

        EPServiceProvider esperSink = EPServiceProviderManager.getDefaultProvider(configuration);

        this.runtime = esperSink.getEPRuntime();
        this.admin = esperSink.getEPAdministrator();

        for (String stmt : statements) {
            EPStatement statement = admin.createEPL(stmt);

            statement.addListener(this);
        }
    }

    private String getEventTypeName(int componentId, int streamId) {
        if (singleEventType) {
            return "Storm";
        }
        else {
            return String.format("Storm_%d_%d", componentId, streamId);
        }
    }
    
    private void setupEventTypes(TopologyContext context, Configuration configuration)
    {
        Set<GlobalStreamId> sourceIds = context.getThisSources().keySet();
        singleEventType = (sourceIds.size() == 1);

        for (GlobalStreamId id : sourceIds) {
            Map<String, Object> props = new LinkedHashMap<String, Object>();

            setupEventTypeProperties(context.getComponentOutputFields(id.get_componentId(), id.get_streamId()), props);
            configuration.addEventType(getEventTypeName(id.get_componentId(), id.get_streamId()), props);
        }
    }

    private void setupEventTypeProperties(Fields fields, Map<String, Object> properties)
    {
        int numFields = fields.size();

        for (int idx = 0; idx < numFields; idx++) {
            properties.put(fields.get(idx), Object.class);
        }
    }
    
    @Override
    public void execute(Tuple tuple)
    {
        String eventType = getEventTypeName(tuple.getSourceComponent(), tuple.getSourceStreamId());
        Map<String, Object> data = new HashMap<String, Object>();
        Fields fields = tuple.getFields();
        int numFields = fields.size();

        for (int idx = 0; idx < numFields; idx++) {
            String name = fields.get(idx);
            Object value = tuple.getValue(idx);

            data.put(name, value);
        }

        runtime.sendEvent(data, eventType);
    }

    @Override
    public void update(EventBean[] newEvents, EventBean[] oldEvents)
    {
        if (newEvents != null) {
            for (EventBean newEvent : newEvents) {
                collector.emit(toTuple(newEvent));
            }
        }
    }

    private List<Object> toTuple(EventBean event)
    {
        int numFields = outputFields.size();
        List<Object> tuple = new ArrayList<Object>(numFields);

        for (int idx = 0; idx < numFields; idx++) {
            tuple.add(event.get(outputFields.get(idx)));
        }
        return tuple;
    }
    
    @Override
    public void cleanup()
    {}
}

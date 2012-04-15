package org.tomdz.storm.esper;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import backtype.storm.generated.GlobalStreamId;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.topology.base.BaseRichBolt;

import com.espertech.esper.client.Configuration;
import com.espertech.esper.client.EPAdministrator;
import com.espertech.esper.client.EPRuntime;
import com.espertech.esper.client.EPServiceProvider;
import com.espertech.esper.client.EPServiceProviderManager;
import com.espertech.esper.client.EPStatement;
import com.espertech.esper.client.EventBean;
import com.espertech.esper.client.UpdateListener;

public class EsperBolt extends BaseRichBolt implements UpdateListener
{
    private static final long serialVersionUID = 1L;

    public static final class Builder
    {
        private final Map<String, String> inputAliases = new LinkedHashMap<String, String>();
        private final Map<String, Fields> eventTypeFieldsMap = new LinkedHashMap<String, Fields>();
        private final Map<String, String> eventTypeStreamIdMap = new LinkedHashMap<String, String>();
        private final List<String> statements = new ArrayList<String>();

        public Builder addInputAlias(String componentId, String name)
        {
            return addInputAlias(componentId, "default", name);
        }

        public Builder addInputAlias(String componentId, String streamId, String name)
        {
            inputAliases.put(componentId + "-" + streamId, name);
            return this;
        }

        public Builder addNamedOutput(String streamId, String eventTypeName, String... fields)
        {
            eventTypeFieldsMap.put(eventTypeName, new Fields(fields));
            eventTypeStreamIdMap.put(eventTypeName, streamId);
            return this;
        }

        public Builder setAnonymousOutput(String streamId, String... fields)
        {
            eventTypeFieldsMap.put(null, new Fields(fields));
            eventTypeStreamIdMap.put(null, streamId);
            return this;
        }

        public Builder addStatement(String stmt)
        {
            statements.add(stmt);
            return this;
        }

        public EsperBolt build()
        {
            return new EsperBolt(inputAliases, eventTypeFieldsMap, eventTypeStreamIdMap, statements);
        }
    }

    private final Map<String, String> inputAliases;
    private final Map<String, Fields> eventTypeFieldsMap;
    private final Map<String, String> eventTypeStreamIdMap;
    private final List<String> statements;
    private transient EPServiceProvider esperSink;
    private transient EPRuntime runtime;
    private transient EPAdministrator admin;
    private transient OutputCollector collector;
    private transient boolean singleEventType;

    private EsperBolt(Map<String, String> inputAliases,
                      Map<String, Fields> eventTypeFieldsMap,
                      Map<String, String> eventTypeStreamIdMap,
                      List<String> statements)
    {
        this.inputAliases = new LinkedHashMap<String, String>(inputAliases);
        this.eventTypeFieldsMap = new LinkedHashMap<String, Fields>(eventTypeFieldsMap);
        this.eventTypeStreamIdMap = new LinkedHashMap<String, String>(eventTypeStreamIdMap);
        this.statements = new ArrayList<String>(statements);
    }

    public List<String> getEventTypes()
    {
        return new ArrayList<String>(eventTypeFieldsMap.keySet());
    }

    public Fields getFieldsForEventType(String eventType)
    {
        return eventTypeFieldsMap.get(eventType);
    }

    public String getStreamIdForEventType(String eventType)
    {
        return eventTypeStreamIdMap.get(eventType);
    }

    public String getEventTypeForStreamId(String streamId)
    {
        for (Map.Entry<String, String> entry : eventTypeStreamIdMap.entrySet()) {
            if (streamId.equals(entry.getValue())) {
                return entry.getKey();
            }
        }
        return null;
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer)
    {
        for (Map.Entry<String, String> entry : eventTypeStreamIdMap.entrySet()) {
            Fields fields = eventTypeFieldsMap.get(entry.getKey());

            declarer.declareStream(entry.getValue(), fields);
        }
    }

    @Override
    public void prepare(@SuppressWarnings("rawtypes") Map conf,
                        TopologyContext context,
                        OutputCollector collector)
    {
        this.collector = collector;

        Configuration configuration = new Configuration();

        setupEventTypes(context, configuration);

        this.esperSink = EPServiceProviderManager.getProvider(this.toString(), configuration);
        this.esperSink.initialize();
        this.runtime = esperSink.getEPRuntime();
        this.admin = esperSink.getEPAdministrator();

        for (String stmt : statements) {
            EPStatement statement = admin.createEPL(stmt);

            statement.addListener(this);
        }
    }

    private String getEventTypeName(String componentId, String streamId)
    {
        String alias = inputAliases.get(componentId + "-" + streamId);

        if (alias == null) {
            alias = String.format("%s_%s", componentId, streamId);
        }
        return alias;
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
                String eventType = newEvent.getEventType().getName();
                String streamId = eventTypeStreamIdMap.get(eventType);

                if (streamId == null) {
                    // anonymous event
                    eventType = null;
                    streamId = eventTypeStreamIdMap.get(null);
                }
                if (streamId != null) {
                    Fields fields = eventTypeFieldsMap.get(eventType);

                    collector.emit(streamId, toTuple(newEvent, fields));
                }
            }
        }
    }

    private List<Object> toTuple(EventBean event, Fields fields)
    {
        int numFields = fields.size();
        List<Object> tuple = new ArrayList<Object>(numFields);

        for (int idx = 0; idx < numFields; idx++) {
            tuple.add(event.get(fields.get(idx)));
        }
        return tuple;
    }
    
    @Override
    public void cleanup()
    {
        if (esperSink != null) {
            esperSink.destroy();
        }
    }
}

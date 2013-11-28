package org.tomdz.storm.esper;

import java.util.ArrayList;
import java.util.Collection;
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
import backtype.storm.tuple.TupleImpl;
import backtype.storm.topology.base.BaseRichBolt;
import com.espertech.esper.client.*;
import com.google.common.collect.ImmutableList;
import java.util.*;


public class EsperBolt extends BaseRichBolt implements UpdateListener
{
    private static final long serialVersionUID = 1L;

    public static class Builder
    {
        protected final EsperBolt bolt;

        public Builder()
        {
            this(new EsperBolt());
        }

        protected Builder(EsperBolt bolt)
        {
            this.bolt = bolt;
        }

        public InputsBuilder inputs()
        {
            return new InputsBuilder(bolt);
        }

        public OutputsBuilder outputs()
        {
            return new OutputsBuilder(bolt);
        }

        public StatementsBuilder statements()
        {
            return new StatementsBuilder(bolt);
        }

        public EsperBolt build()
        {
            return bolt;
        }
    }

    public static class InputsBuilder extends Builder
    {
        private InputsBuilder(EsperBolt bolt)
        {
            super(bolt);
        }

        public AliasedInputBuilder aliasComponent(String componentId)
        {
            return new AliasedInputBuilder(bolt, new StreamId(componentId));
        }

        public AliasedInputBuilder aliasStream(String componentId, String streamId)
        {
            return new AliasedInputBuilder(bolt, new StreamId(componentId, streamId));
        }
    }

    public static final class AliasedInputBuilder
    {
        private final EsperBolt bolt;
        private final StreamId streamId;
        private final Map<String, String> fieldTypes;

        private AliasedInputBuilder(EsperBolt bolt, StreamId streamId)
        {
            this(bolt, streamId, new HashMap<String, String>());
        }

        private AliasedInputBuilder(EsperBolt bolt, StreamId streamId, Map<String, String> fieldTypes)
        {
            this.bolt = bolt;
            this.streamId = streamId;
            this.fieldTypes = fieldTypes;
        }

        public TypedInputBuilder withField(String fieldNames)
        {
            return new TypedInputBuilder(bolt, streamId, fieldTypes, fieldNames);
        }

        public TypedInputBuilder withFields(String... fieldNames)
        {
            return new TypedInputBuilder(bolt, streamId, fieldTypes, fieldNames);
        }

        public InputsBuilder toEventType(String name)
        {
            bolt.addInputAlias(streamId, name, new TupleTypeDescriptor(fieldTypes));
            return new InputsBuilder(bolt);
        }
    }

    public static final class TypedInputBuilder
    {
        private final EsperBolt bolt;
        private final StreamId streamId;
        private final Map<String, String> fieldTypes;
        private final String[] fieldNames;

        private TypedInputBuilder(EsperBolt bolt, StreamId streamId, Map<String, String> fieldTypes, String... fieldNames)
        {
            this.bolt = bolt;
            this.streamId = streamId;
            this.fieldTypes = fieldTypes;
            this.fieldNames = fieldNames;
        }

        public AliasedInputBuilder ofType(Class<?> type)
        {
            for (String fieldName : fieldNames) {
                fieldTypes.put(fieldName, type.getName());
            }
            return new AliasedInputBuilder(bolt, streamId, fieldTypes);
        }
    }

    public static final class OutputsBuilder extends Builder
    {
        private OutputsBuilder(EsperBolt bolt)
        {
            super(bolt);
        }

        public OutputStreamBuilder onStream(String streamName)
        {
            return new OutputStreamBuilder(bolt, streamName);
        }

        public OutputStreamBuilder onDefaultStream()
        {
            return new OutputStreamBuilder(bolt, "default");
        }
    }

    public static final class OutputStreamBuilder
    {
        private final EsperBolt bolt;
        private final String streamName;

        private OutputStreamBuilder(EsperBolt bolt, String streamName)
        {
            this.bolt = bolt;
            this.streamName = streamName;
        }

        public NamedOutputStreamBuilder fromEventType(String name)
        {
            return new NamedOutputStreamBuilder(bolt, streamName, name);
        }

        public OutputsBuilder emit(String... fields)
        {
            bolt.setAnonymousOutput(streamName, fields);
            return new OutputsBuilder(bolt);
        }
    }

    public static final class NamedOutputStreamBuilder
    {
        private final EsperBolt bolt;
        private final String streamName;
        private final String eventTypeName;

        private NamedOutputStreamBuilder(EsperBolt bolt, String streamName, String eventTypeName)
        {
            this.bolt = bolt;
            this.streamName = streamName;
            this.eventTypeName = eventTypeName;
        }

        public OutputsBuilder emit(String... fields)
        {
            bolt.addNamedOutput(streamName, eventTypeName, fields);
            return new OutputsBuilder(bolt);
        }
    }

    public static final class StatementsBuilder extends Builder
    {
        private StatementsBuilder(EsperBolt bolt)
        {
            super(bolt);
        }

        public StatementsBuilder add(String statement)
        {
            bolt.addStatement(statement);
            return this;
        }
    }

    private final Map<StreamId, String> inputAliases = new LinkedHashMap<StreamId, String>();
    private final Map<StreamId, TupleTypeDescriptor> tupleTypes = new LinkedHashMap<StreamId, TupleTypeDescriptor>();
    private final Map<String, EventTypeDescriptor> eventTypes = new LinkedHashMap<String, EventTypeDescriptor>();
    private final List<String> statements = new ArrayList<String>();
    private transient EPServiceProvider esperSink;
    private transient EPRuntime runtime;
    private transient EPAdministrator admin;
    private transient OutputCollector collector;
    private transient TopologyContext context;

    private EsperBolt()
    {
    }

    private void addInputAlias(StreamId streamId, String name, TupleTypeDescriptor typeDesc)
    {
        inputAliases.put(streamId, name);
        if (typeDesc != null) {
            tupleTypes.put(streamId, typeDesc);
        }
    }

    private void addNamedOutput(String streamId, String eventTypeName, String... fields)
    {
        eventTypes.put(eventTypeName, new EventTypeDescriptor(eventTypeName, fields, streamId));
    }

    private void setAnonymousOutput(String streamId, String... fields)
    {
        eventTypes.put(null, new EventTypeDescriptor(null, fields, streamId));
    }

    private void addStatement(String stmt)
    {
        statements.add(stmt);
    }

    public EventTypeDescriptor getEventType(String name)
    {
        return eventTypes.get(name);
    }

    public Collection<EventTypeDescriptor> getEventTypes()
    {
        return new ArrayList<EventTypeDescriptor>(eventTypes.values());
    }

    public EventTypeDescriptor getEventTypeForStreamId(String streamId)
    {
        for (EventTypeDescriptor eventType: eventTypes.values()) {
            if (streamId.equals(eventType.getStreamId())) {
                return eventType;
            }
        }
        return null;
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer)
    {
        for (EventTypeDescriptor eventType: eventTypes.values()) {
            declarer.declareStream(eventType.getStreamId(), eventType.getFields());
        }
    }

    @Override
    public void prepare(@SuppressWarnings("rawtypes") Map conf,
                        TopologyContext context,
                        OutputCollector collector)
    {
        this.collector = collector;
        this.context = context;

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
        String alias = inputAliases.get(new StreamId(componentId, streamId));

        if (alias == null) {
            alias = String.format("%s_%s", componentId, streamId);
        }
        return alias;
    }

    private void setupEventTypes(TopologyContext context, Configuration configuration)
    {
        Set<GlobalStreamId> sourceIds = context.getThisSources().keySet();

        for (GlobalStreamId id : sourceIds) {
            String eventTypeName = getEventTypeName(id.get_componentId(), id.get_streamId());
            Fields fields = context.getComponentOutputFields(id.get_componentId(), id.get_streamId());
            TupleTypeDescriptor typeDesc = tupleTypes.get(new StreamId(id.get_componentId(), id.get_streamId()));
            Map<String, Object> props = setupEventTypeProperties(fields, typeDesc);

            configuration.addEventType(eventTypeName, props);
        }
    }

    private Map<String, Object> setupEventTypeProperties(Fields fields, TupleTypeDescriptor typeDesc)
    {
        Map<String, Object> properties = new HashMap<String, Object>();
        int numFields = fields.size();

        for (int idx = 0; idx < numFields; idx++) {
            String fieldName = fields.get(idx);
            Class<?> clazz = null;

            if (typeDesc != null) {
                String clazzName = typeDesc.getFieldType(fieldName);

                if (clazzName != null) {
                    try {
                        clazz = Class.forName(clazzName);
                    }
                    catch (ClassNotFoundException ex) {
                        throw new RuntimeException("Cannot find class " + clazzName + "declared for field " + fieldName);
                    }
                }
            }
            if (clazz == null) {
                clazz = Object.class;
            }
            properties.put(fieldName, clazz);
        }
        return properties;
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
        collector.ack(tuple);
    }

    @Override
    public void update(EventBean[] newEvents, EventBean[] oldEvents)
    {
        if (newEvents != null) {
            for (EventBean newEvent : newEvents) {
                EventTypeDescriptor eventType = getEventType(newEvent.getEventType().getName());

                if (eventType == null) {
                    // anonymous event ?
                    eventType = getEventType(null);
                }
                if (eventType != null) {
                    collector.emit(
                            eventType.getStreamId(),
                            createAnchorTuples(eventType),
                            toTuple(newEvent, eventType.getFields()));
                }
            }
        }
    }

    private Collection<Tuple> createAnchorTuples(EventTypeDescriptor eventType) {
        ImmutableList.Builder<Object> builder = ImmutableList.<Object>builder();
        for (int i = 0; i < eventType.getFields().size(); i++) {
            builder.add(UUID.randomUUID());
        }
        List<Object> tupleObjects = builder.build();
        Tuple tuple =  new TupleImpl(
                    this.context,
                    tupleObjects,
                    context.getThisTaskId(),
                    eventType.getStreamId());
        return  ImmutableList.<Tuple>of(tuple);
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

package org.tomdz.storm.esper;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

public class Event
{
    private final String componentId;
    private final String streamId;
    private final String type;
    private final List<?> data;

    public Event(String componentId, String streamId, String type, Object... data)
    {
        this.componentId = componentId;
        this.streamId = streamId;
        this.type = type;
        this.data = Collections.unmodifiableList(Arrays.asList(data));
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        Event event = (Event) o;

        if (!componentId.equals(event.componentId)) {
            return false;
        }
        if (!data.equals(event.data)) {
            return false;
        }
        if (!streamId.equals(event.streamId)) {
            return false;
        }
        if (type != null ? !type.equals(event.type) : event.type != null) {
            return false;
        }

        return true;
    }

    @Override
    public int hashCode()
    {
        int result = componentId.hashCode();
        result = 31 * result + streamId.hashCode();
        result = 31 * result + (type != null ? type.hashCode() : 0);
        result = 31 * result + data.hashCode();
        return result;
    }

    @Override
    public String toString()
    {
        return "Event[" +
                "componentId='" + componentId + '\'' +
                ", streamId='" + streamId + '\'' +
                ", type='" + type + '\'' +
                ", data=" + data +
                ']';
    }
}
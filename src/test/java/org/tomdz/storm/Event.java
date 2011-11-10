package org.tomdz.storm;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

public class Event
{
    public final String type;
    public final List<?> data;

    public Event(String type, Object... data)
    {
        this.type = type;
        this.data = Collections.unmodifiableList(Arrays.asList(data));
    }
}
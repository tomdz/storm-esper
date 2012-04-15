package org.tomdz.storm.esper;

public class Connection
{
    private final String sourceComponent;
    private final String sourceStream;
    private final String targetComponent;

    public Connection(String sourceComponent, String sourceStream, String targetComponent)
    {
        this.sourceComponent = sourceComponent;
        this.sourceStream = sourceStream;
        this.targetComponent = targetComponent;
    }

    public Connection(String sourceComponent, String targetComponent)
    {
        this(sourceComponent, "default", targetComponent);
    }

    public String getSourceComponent()
    {
        return sourceComponent;
    }

    public String getSourceStream()
    {
        return sourceStream;
    }

    public String getTargetComponent()
    {
        return targetComponent;
    }
}

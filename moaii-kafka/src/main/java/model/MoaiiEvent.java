package model;

import java.time.LocalTime;

/**
 * Moaii event model
 */
public class MoaiiEvent
{
    private int incidenceId;
    private int eventId;
    private LocalTime timestamp;

    public int getIncidenceId(){return incidenceId;}
    public void setIncidenceId(int incidenceId){this.incidenceId = incidenceId;}

    public Integer getEventId()
    {
        return eventId;
    }
    public void setEventId(int eventId)
    {
        this.eventId = eventId;
    }


    public LocalTime getTimestamp()
    {
        return timestamp;
    }
    public void setTimestamp(LocalTime timestamp)
    {
        this.timestamp = timestamp;
    }

    @Override
    public String toString()
    {
        return "MoaiiEvent{" +
                "incidenceId=" + incidenceId +
                ", eventId=" + eventId +
                ", timestamp=" + timestamp +
                '}';
    }
}

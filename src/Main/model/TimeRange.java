package Main.model;

public class TimeRange {
    public long start;
    public long end;

    public static TimeRange of(long start, long end) {
        TimeRange timeRange = new TimeRange();
        timeRange.start = start;
        timeRange.end = end;
        return timeRange;
    }

    public boolean Equals(TimeRange other){
        return start == other.start && end == other.end;
    }
    Duration getDuration(){
        return new Duration(end - start);
    }
    
    TimeRange Merge(TimeRange other){
        if (start == 0 && end == 0){
            return other;
        }
        return TimeRange.of(Math.min(start,other.start),Math.max(end,other.end));
    }
}
package Main.model;

public class TimeRecord {
    public long timestamp;
    public double value;
    static TimeRecord of(long a, double b){
        TimeRecord newRecord = new TimeRecord();
        newRecord.timestamp = a;
        newRecord.value = b;
        return newRecord;
    }
}

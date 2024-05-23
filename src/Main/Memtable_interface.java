package Main;

import java.util.ArrayList;

import Main.model.Options;
import Main.model.ReadResult;
import Main.model.TimeRange;
import Main.model.TimeRecord;
import Main.column.*;

public interface Memtable_interface {
    void Init(Options options, ArrayList<StoredAggregationType> metricOptions) throws Throwable;
    void Finish() throws Throwable;
   public void Write(ArrayList<TimeRecord> inputSeries) throws Throwable;
   ReadResult Read(TimeRange range, StoredAggregationType type) throws Throwable;
}
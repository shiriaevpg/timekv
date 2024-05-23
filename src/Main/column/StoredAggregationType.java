package Main.column;

enum ColumnType{
    SUM,COUNT,MIN,MAX,LAST,RAWTIMESTAMPS,RAWVALUES,RAWREAD,AVG;
    public static ColumnType getDefault(){
        return SUM;
    }
}


public enum StoredAggregationType {
    kNone(0),
    kSum(1),
    kCount(2),
    kMin(3),
    kMax(4),
    kLast(5);
    private int value;
    StoredAggregationType(int i) {
        this.value = i;
    }
    static StoredAggregationType getDefault(){
        return kNone;
    }
    public int getValue(){
        return value;
    }
  };
  
  // WARNING: preserve order like in StoredAggregationType to make it easier to
  // convert between them
  enum AggregationType {
    kNone,
    kSum,
    kCount,
    kMin,
    kMax,
    kLast,
    kAvg;
    static AggregationType getDefault(){
        return kNone;
    }
  };
  
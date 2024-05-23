package Main;

import java.lang.foreign.Arena;
import java.lang.foreign.FunctionDescriptor;
import java.lang.foreign.Linker;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.SymbolLookup;
import java.lang.foreign.ValueLayout;
import java.lang.invoke.MethodHandle;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;

import Main.column.StoredAggregationType;
import Main.model.ReadResult;
import Main.model.TimeRange;
import Main.model.TimeRecord;
import Main.model.Options;

public
class Memtable implements Memtable_interface {
  final static Linker linker = Linker.nativeLinker();
  Path testFilePath;
  Arena arena;
  SymbolLookup myLib;
  MethodHandle init, getOptions, getMetricOptions, write, read, stop,
      parseColumn, getValSize, getArr, buildTimeRange, getRecords;

  Memtable() {
    arena = Arena.ofConfined();
    testFilePath = Paths.get("libmemtable.so");
    myLib = SymbolLookup.libraryLookup(testFilePath, arena);
    init = linker.downcallHandle(
        myLib.find("tskvInit").get(),
        FunctionDescriptor.ofVoid(ValueLayout.ADDRESS, ValueLayout.ADDRESS));
    getOptions = linker.downcallHandle(
        myLib.find("tskvgetOptions").get(),
        FunctionDescriptor.of(ValueLayout.ADDRESS, ValueLayout.JAVA_LONG,
                              ValueLayout.JAVA_LONG, ValueLayout.JAVA_LONG,
                              ValueLayout.JAVA_BOOLEAN));
    getMetricOptions = linker.downcallHandle(
        myLib.find("tskvGetMetricOptions").get(),
        FunctionDescriptor.of(ValueLayout.ADDRESS, ValueLayout.ADDRESS,
                              ValueLayout.JAVA_LONG));
    write = linker.downcallHandle(
        myLib.find("tskvWrite").get(),
        FunctionDescriptor.ofVoid(ValueLayout.ADDRESS, ValueLayout.JAVA_LONG));
    read = linker.downcallHandle(
        myLib.find("tskvRead").get(),
        FunctionDescriptor.of(ValueLayout.ADDRESS, ValueLayout.ADDRESS,
                              ValueLayout.JAVA_INT));
    stop = linker.downcallHandle(myLib.find("tskvStop").get(),
                                 FunctionDescriptor.ofVoid());
    parseColumn =
        linker.downcallHandle(myLib.find("tskvParseColumn").get(),
                              FunctionDescriptor.ofVoid(ValueLayout.ADDRESS));
    getValSize =
        linker.downcallHandle(myLib.find("tskvGetValSize").get(),
                              FunctionDescriptor.of(ValueLayout.JAVA_LONG));
    getArr = linker.downcallHandle(myLib.find("tskvGetArr").get(),
                                   FunctionDescriptor.of(ValueLayout.ADDRESS));
    buildTimeRange = linker.downcallHandle(
        myLib.find("tskvBuildTimeRange").get(),
        FunctionDescriptor.of(ValueLayout.ADDRESS, ValueLayout.JAVA_LONG,
                              ValueLayout.JAVA_LONG));
    getRecords = linker.downcallHandle(
        myLib.find("tskvGetRecords").get(),
        FunctionDescriptor.of(ValueLayout.ADDRESS, ValueLayout.ADDRESS,
                              ValueLayout.ADDRESS, ValueLayout.JAVA_LONG,
                              ValueLayout.JAVA_LONG));
  }

  @Override 
  public void Init(
      Options options,
      ArrayList<StoredAggregationType> metricOptions) throws Throwable {
    long sz = metricOptions.size();
    MemorySegment optionsArr = arena.allocate((long)(sz * 4), 1);
    for (int i = 0; i < sz; i++) {
      optionsArr.setAtIndex(ValueLayout.JAVA_INT, i,
                            metricOptions.get(i).getValue());
    }
    init.invoke(getOptions.invoke(options.interval, options.maxBytesSize,
                                  options.maxAge, options.store_raw),
                getMetricOptions.invoke(optionsArr, sz));
  }

  @Override 
  public void Finish() throws Throwable {
    stop.invoke();
  }

  @Override 
  public void Write(
      ArrayList<TimeRecord> inputSeries) throws Throwable {
    int sz = inputSeries.size();
    long lsz = sz;
    MemorySegment a = arena.allocate((long)(sz * 8), 1);
    MemorySegment b = arena.allocate((long)(sz * 8), 1);
    for (int i = 0; i < sz; i++) {
      a.setAtIndex(ValueLayout.JAVA_LONG, i, inputSeries.get(i).timestamp);
      b.setAtIndex(ValueLayout.JAVA_DOUBLE, i, inputSeries.get(i).value);
    }
    MemorySegment records = (MemorySegment)getRecords.invoke(a, b, lsz);
    write.invoke(records, sz);
  }

  @Override 
  public ReadResult Read(
      TimeRange range, StoredAggregationType type) throws Throwable {
    MemorySegment timeRange =
        (MemorySegment)buildTimeRange.invoke(range.start, range.end);
    read.invoke(timeRange, type.getValue());
    long sz = (long)getValSize.invoke();
    MemorySegment vals = (MemorySegment)getArr.invoke();
    ArrayList<Double> values = new ArrayList<Double>((int)sz);
    for (int i = 0; i < sz; i++) {
      values.set(i, (double)vals.getAtIndex(ValueLayout.JAVA_DOUBLE, i));
    }

    return new ReadResult(values);
  }
}

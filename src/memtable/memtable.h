#pragma once

#include <array>
#include <chrono>
#include <cstdint>
#include <iterator>
#include <memory>
#include <optional>
#include <vector>

#include "../model/column.h"
#include "../model/model.h"

namespace tskv {

struct MetricOptions;

class Memtable {
 public:
  struct Options {
    Duration bucket_interval;
    std::optional<size_t> max_bytes_size;
    std::optional<Duration> max_age;
    bool store_raw{false};
  };

  struct ReadResult {
    Column found;
    // only <= 1 time range, because memtable stores data suffix
    std::optional<TimeRange> not_found;
  };

 public:
  Memtable(const Options& options, const MetricOptions& metric_options);
  void Write(const InputTimeSeries& time_series);
  ReadResult Read(const TimeRange& time_range,
                  StoredAggregationType aggregation_type) const;
  Columns ExtractColumns();
  bool NeedFlush() const;

 private:
  ReadResult ReadRawValues(const TimeRange& time_range) const;

  size_t GetBytesSize() const;

  Columns columns_;
  Options options_;
};

}  // namespace tskv

extern "C" {
void tskvInit(tskv::Memtable::Options* options,
              tskv::MetricOptions* metric_options);
void tskvWrite(tskv::Record* data, long long sz);
tskv::Memtable::ReadResult* Read(tskv::TimeRange*,
                                 tskv::StoredAggregationType*);
void tskvStop();
void tskvParseColumn(tskv::Column*);
long long tskvGetValSize();
double* tskvGetArr();
tskv::TimeRange* tskvBuildTimeRange(long long, long long);
tskv::Memtable::Options* tskvGetOptions(long long interval, long long sz,
                                        long long maxage, bool raw);
tskv::MetricOptions* tskvGetMetricOptions(int* arr, long long sz);
}
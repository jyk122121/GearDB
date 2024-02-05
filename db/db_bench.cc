// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include <sys/types.h>
#include <stdio.h>
#include <stdlib.h>
#include "db/db_impl.h"
#include "db/version_set.h"
#include "leveldb/cache.h"
#include "leveldb/db.h"
#include "leveldb/env.h"
#include "leveldb/write_batch.h"
#include "port/port.h"
#include "util/crc32c.h"
#include "util/histogram.h"
#include "util/mutexlock.h"
#include "util/random.h"
#include "util/testutil.h"

#include "../hm/get_manager.h"
#include "util/generator.h"

#define SLEEP_WAIT_FILL 0  //means waiting for compaction after fillrandom to balance the data of each level; 
                          //0 means not waiting

// Comma-separated list of operations to run in the specified order
//   Actual benchmarks:
//      fillseq       -- write N values in sequential key order in async mode
//      fillrandom    -- write N values in random key order in async mode
//      overwrite     -- overwrite N values in random key order in async mode
//      fillsync      -- write N/100 values in random key order in sync mode
//      fill100K      -- write N/1000 100K values in random order in async mode
//      deleteseq     -- delete N keys in sequential order
//      deleterandom  -- delete N keys in random order
//      readseq       -- read N times sequentially
//      readreverse   -- read N times in reverse order
//      readrandom    -- read N times in random order
//      readmissing   -- read N missing keys in random order
//      readhot       -- read N times in random order from 1% section of DB
//      seekrandom    -- N random seeks
//      open          -- cost of opening a DB
//      crc32c        -- repeated crc32c of 4K of data
//      acquireload   -- load N*1000 times
//   Meta operations:
//      compact     -- Compact the entire DB
//      stats       -- Print DB stats
//      sstables    -- Print sstable info
//      heapprofile -- Dump a heap profile (if supported by this port)
static const char* FLAGS_benchmarks =
    "fillseq,"
    //"fillsync,"
    "fillrandom,"
    "stats,"
    "overwrite,"
    "readseq,"
    //"readseq,"
    //"readseq,"
    "readrandom,"
    //"readseq,"
    //"readseq,"
    //"readrandom,"
    "A,"
    "B,"
    "C,"
    "D,"
    "E,"
    "F,"
    "stats,"
    ;
  //  "overwrite,"
//    "readrandom,"
//   "readrandom,"  // Extra run to allow previous compactions to quiesce
 /*   "readseq,"
    "readreverse,"
    "compact,"
    "readrandom,"
    "readseq,"
    "readreverse,"
    "fill100K,"
    "crc32c,"
    "snappycomp,"
    "snappyuncomp,"
    "acquireload,"
    ;*/

// Number of key/values to place in database
static int FLAGS_num = 100000;
//      10G = 2621440;
//      20G = 5242880;
//      40G = 10485760;
//      60G = 15728640;
//      80G = 20971520;
//     100G = 26214400;

// Number of read operations to do.  If negative, do FLAGS_num reads.
static int FLAGS_reads = 100000;
static int FLAGS_reads_seq = 100000;

static int FLAGS_ops = 10000000;
// Number of concurrent threads to run.
static int FLAGS_threads = 1;

// Size of each value
static int FLAGS_value_size = 4096;

// Arrange to generate values that shrink to this fraction of
// their original size after compression
static double FLAGS_compression_ratio = 0.5;

// Print histogram of operation timings
static bool FLAGS_histogram = false;

// Number of bytes to buffer in memtable before compacting
// (initialized to default value by "main")
static int FLAGS_write_buffer_size = 0;

// Number of bytes written to each file.
// (initialized to default value by "main")
static int FLAGS_max_file_size = 0;

// Approximate size of user data packed per block (before compression.
// (initialized to default value by "main")
static int FLAGS_block_size = 0;

// Number of bytes to use as a cache of uncompressed data.
// Negative means use default settings.
static int FLAGS_cache_size = -1;

// Maximum number of files to keep open at the same time (use default if == 0)
static int FLAGS_open_files = 0;

// Bloom filter bits per key.
// Negative means use default settings.
static int FLAGS_bloom_bits = -1;

// If true, do not destroy the existing database.  If you set this
// flag and also specify a benchmark that wants a fresh database, that
// benchmark will fail.
static bool FLAGS_use_existing_db = false;

// If true, reuse existing log/MANIFEST files when re-opening a database.
static bool FLAGS_reuse_logs = false;

// Use the db with the following name.
static const char* FLAGS_db = NULL;

namespace leveldb {
using namespace util;

namespace {
leveldb::Env* g_env = NULL;

// Helper for quickly generating random data.
class RandomGenerator {
 private:
  std::string data_;
  int pos_;

 public:
  RandomGenerator() {
    // We use a limited amount of data over and over again and ensure
    // that it is larger than the compression window (32KB), and also
    // large enough to serve all typical value sizes we want to write.
    Random rnd(301);
    std::string piece;
    while (data_.size() < 1048576) { // 1MB
      // Add a short fragment that is as compressible as specified
      // by FLAGS_compression_ratio.
      test::CompressibleString(&rnd, FLAGS_compression_ratio, 100, &piece);
      data_.append(piece);
    }
    pos_ = 0;
  }

  Slice Generate(size_t len) {
    if (pos_ + len > data_.size()) {
      pos_ = 0;
      assert(len < data_.size());
    }
    pos_ += len;
    return Slice(data_.data() + pos_ - len, len);
  }
};

#if defined(__linux)
static Slice TrimSpace(Slice s) {
  size_t start = 0;
  while (start < s.size() && isspace(s[start])) {
    start++;
  }
  size_t limit = s.size();
  while (limit > start && isspace(s[limit-1])) {
    limit--;
  }
  return Slice(s.data() + start, limit - start);
}
#endif

static void AppendWithSpace(std::string* str, Slice msg) {
  if (msg.empty()) return;
  if (!str->empty()) {
    str->push_back(' ');
  }
  str->append(msg.data(), msg.size());
}

// class CombinedStats;
class Stats {
 private:
  double finish_;
  double seconds_;
  int done_;
  int next_report_;
  int64_t bytes_;
  double last_op_finish_;
  Histogram hist_;
  std::string message_;

  double throughput;

  // friend class CombinedStats;

 public:
 double start_;
  Stats() { Start(); }

  void Start() {
    next_report_ = 100;
    last_op_finish_ = start_;
    hist_.Clear();
    done_ = 0;
    bytes_ = 0;
    seconds_ = 0;
    start_ = g_env->NowMicros();
    finish_ = start_;
    message_.clear();

    throughput = 0;
  }

  void Merge(const Stats& other) {
    hist_.Merge(other.hist_);
    done_ += other.done_;
    bytes_ += other.bytes_;
    seconds_ += other.seconds_;
    if (other.start_ < start_) start_ = other.start_;
    if (other.finish_ > finish_) finish_ = other.finish_;



    // Just keep the messages from one thread
    if (message_.empty()) message_ = other.message_;
  }

  void Stop() {
    finish_ = g_env->NowMicros();
    seconds_ = (finish_ - start_) * 1e-6;
  }

  void AddMessage(Slice msg) {
    AppendWithSpace(&message_, msg);
  }

  void FinishedSingleOp() {
    if (FLAGS_histogram) {
      double now = g_env->NowMicros();
      double micros = now - last_op_finish_;
      hist_.Add(micros);
      if (micros > 20000) {
        fprintf(stderr, "long op: %.1f micros%30s\r", micros, "");
        fflush(stderr);
      }
      last_op_finish_ = now;
    }

    done_++;
    if (done_ >= next_report_) {
      if      (next_report_ < 1000)   next_report_ += 100;
      else if (next_report_ < 5000)   next_report_ += 500;
      else if (next_report_ < 10000)  next_report_ += 1000;
      else if (next_report_ < 50000)  next_report_ += 5000;
      else if (next_report_ < 100000) next_report_ += 10000;
      else if (next_report_ < 500000) next_report_ += 50000;
      else                            next_report_ += 100000;
      fprintf(stderr, "... finished %d ops%30s\r", done_, "");
      fflush(stderr);
    }
  }

  void AddBytes(int64_t n) {
    bytes_ += n;
  }

  void Report(const Slice& name) {
    // Pretend at least one op was done in case we are running a benchmark
    // that does not call FinishedSingleOp().
    if (done_ < 1) done_ = 1;

    std::string extra;
    double elapsed = (finish_ - start_) * 1e-6;

    if (bytes_ > 0) {
      // Rate is computed on actual elapsed time, not the sum of per-thread
      // elapsed times.
      char rate[100];
      snprintf(rate, sizeof(rate), "%6.1f MB/s",
               (bytes_ / 1048576.0) / elapsed);
      extra = rate;
    }
    AppendWithSpace(&extra, message_);
    double throughput = (double)done_ / elapsed;

    fprintf(stdout, "%-12s : %11.3f micros/op;%s%s  time:%.2f s\n",
            name.ToString().c_str(),
            seconds_ * 1e6 / done_,
            (extra.empty() ? "" : " "),
            extra.c_str(),seconds_);
    
    // fprintf(stdout, "throughput : %lf\n", done_ / seconds_);
    fprintf(stdout, "throughput : %ld ops/s\n", (long)throughput);
    
    if (FLAGS_histogram) {
      fprintf(stdout, "Microseconds per op:\n%s\n", hist_.ToString().c_str());
    }
    fflush(stdout);
  }
};

// class CombinedStats {
//   public:
//     void AddStats(const Stats &stat) {
//       uint64_t total_ops = stat.done_;
//       uint64_t total_bytes_ = stat.bytes_;
//       double elapsed;

//       if (total_ops < 1)
//         total_ops = 1;
      
//       elapsed = (stat.finish_ - stat.start_) * 1e-6;
//       throughput_ops_.emplace_back(total_ops / elapsed);

//       if (total_bytes_ > 0) {
//         double mbs = (total_bytes_ / 1048576.0);
//         throughput_mbs_.emplace_back(mbs / elapsed);
//       }
//     }

//     void Report(const std::string& bench_name) {

//     }
  
//   private:
//     std::vector<double> throughput_ops_;
//     std::vector<double> throughput_mbs_;
// };

// State shared by all concurrent executions of the same benchmark.
struct SharedState {
  port::Mutex mu;
  port::CondVar cv;
  int total;

  // Each thread goes through the following states:
  //    (1) initializing
  //    (2) waiting for others to be initialized
  //    (3) running
  //    (4) done

  int num_initialized;
  int num_done;
  bool start;

  SharedState() : cv(&mu) { }
};

// Per-thread state for concurrent executions of the same benchmark.
struct ThreadState {
  int tid;             // 0..n-1 when running in n threads
  Random rand;         // Has different seeds for different threads
  Stats stats;
  SharedState* shared;

  ThreadState(int index)
      : tid(index),
        rand(1000 + index) {
  }
};

}  // namespace

class Benchmark {
 private:
  Cache* cache_;
  const FilterPolicy* filter_policy_;
  DB* db_;
  int num_;
  int value_size_;
  int entries_per_batch_;
  WriteOptions write_options_;
  int reads_;
  int heap_counter_;

  HMManager *hm_manager_;

  uint64_t ops_;
  CounterGenerator *key_sequence_; // counter generator -> load operation
  Generator<uint64_t> *key_chooser_;
  Generator<uint64_t> *scan_length_; // uniform generator
  DiscreteGenerator<util::DBOperation> operation_chooser_;
  AcknowledgedCounterGenerator *txn_insert_key_seq_;
  Generator<uint64_t> *field_len_generator_;
  Generator<uint64_t> *field_chooser_;

  int zero_padding_ = 1;
  int field_count_ = 10; // default : 10
  std::string field_prefix_ = "field"; // default : field
  bool read_all_fields_ = true; // default : true
  bool write_all_fields_ = false; // default : false
  bool ordered_insert_ = false;
  int field_len_ = 100;

  struct Field {
    std::string name;
    std::string value;
  };

  void PrintHeader() {
    // const int kKeySize = 16;
    const int kKeySize = 20;
    PrintEnvironment();
    fprintf(stdout, "Keys:       %d bytes each\n", kKeySize);
    fprintf(stdout, "Values:     %d bytes each (%d bytes after compression)\n",
            FLAGS_value_size,
            static_cast<int>(FLAGS_value_size * FLAGS_compression_ratio + 0.5));
    fprintf(stdout, "Entries:    %d\n", num_);
    fprintf(stdout, "RawSize:    %.1f MB (estimated)\n",
            ((static_cast<int64_t>(kKeySize + FLAGS_value_size) * num_)
             / 1048576.0));
    fprintf(stdout, "FileSize:   %.1f MB (estimated)\n",
            (((kKeySize + FLAGS_value_size * FLAGS_compression_ratio) * num_)
             / 1048576.0));
    PrintWarnings();
    fprintf(stdout, "------------------------------------------------\n");
  }

  void PrintWarnings() {
#if defined(__GNUC__) && !defined(__OPTIMIZE__)
    fprintf(stdout,
            "WARNING: Optimization is disabled: benchmarks unnecessarily slow\n"
            );
#endif
#ifndef NDEBUG
    fprintf(stdout,
            "WARNING: Assertions are enabled; benchmarks unnecessarily slow\n");
#endif

    // See if snappy is working by attempting to compress a compressible string
    const char text[] = "yyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyy";
    std::string compressed;
    if (!port::Snappy_Compress(text, sizeof(text), &compressed)) {
      fprintf(stdout, "WARNING: Snappy compression is not enabled\n");
    } else if (compressed.size() >= sizeof(text)) {
      fprintf(stdout, "WARNING: Snappy compression is not effective\n");
    }
  }

  void PrintEnvironment() {
    fprintf(stderr, "LevelDB:    version %d.%d\n",
            kMajorVersion, kMinorVersion);

#if defined(__linux)
    time_t now = time(NULL);
    fprintf(stderr, "Date:       %s", ctime(&now));  // ctime() adds newline

    FILE* cpuinfo = fopen("/proc/cpuinfo", "r");
    if (cpuinfo != NULL) {
      char line[1000];
      int num_cpus = 0;
      std::string cpu_type;
      std::string cache_size;
      while (fgets(line, sizeof(line), cpuinfo) != NULL) {
        const char* sep = strchr(line, ':');
        if (sep == NULL) {
          continue;
        }
        Slice key = TrimSpace(Slice(line, sep - 1 - line));
        Slice val = TrimSpace(Slice(sep + 1));
        if (key == "model name") {
          ++num_cpus;
          cpu_type = val.ToString();
        } else if (key == "cache size") {
          cache_size = val.ToString();
        }
      }
      fclose(cpuinfo);
      fprintf(stderr, "CPU:        %d * %s\n", num_cpus, cpu_type.c_str());
      fprintf(stderr, "CPUCache:   %s\n", cache_size.c_str());
    }
#endif
  }

 public:
  Benchmark()
  : cache_(FLAGS_cache_size >= 0 ? NewLRUCache(FLAGS_cache_size) : NULL),
    filter_policy_(FLAGS_bloom_bits >= 0
                   ? NewBloomFilterPolicy(FLAGS_bloom_bits)
                   : NULL),
    db_(NULL),
    num_(FLAGS_num),
    value_size_(FLAGS_value_size),
    entries_per_batch_(1),
    reads_(FLAGS_reads < 0 ? FLAGS_num : FLAGS_reads),
    heap_counter_(0),
    hm_manager_(Singleton::Gethmmanager()) {
    std::vector<std::string> files;
    g_env->GetChildren(FLAGS_db, &files);
    for (size_t i = 0; i < files.size(); i++) {
      if (Slice(files[i]).starts_with("heap-")) {
        g_env->DeleteFile(std::string(FLAGS_db) + "/" + files[i]);
      }
    }
    if (!FLAGS_use_existing_db) {
      DestroyDB(FLAGS_db, Options());
    }
  }

  ~Benchmark() {
    delete db_;
    delete cache_;
    delete filter_policy_;
  }

  void Run() {
    PrintHeader();
    Open();

    const char* benchmarks = FLAGS_benchmarks;
    while (benchmarks != NULL) {
      const char* sep = strchr(benchmarks, ',');
      Slice name;
      if (sep == NULL) {
        name = benchmarks;
        benchmarks = NULL;
      } else {
        name = Slice(benchmarks, sep - benchmarks);
        benchmarks = sep + 1;
      }

      // Reset parameters that may be overridden below
      num_ = FLAGS_num;
      reads_ = (FLAGS_reads < 0 ? FLAGS_num : FLAGS_reads);
      value_size_ = FLAGS_value_size;
      entries_per_batch_ = 1;
      write_options_ = WriteOptions();
      ops_ = FLAGS_ops;

      void (Benchmark::*method)(ThreadState*) = NULL;
      bool fresh_db = false;
      int num_threads = FLAGS_threads;

      field_len_generator_ = new ConstGenerator(field_len_);
      field_chooser_ = new UniformGenerator(0, field_count_ - 1);
      key_sequence_ = new CounterGenerator(0);
      txn_insert_key_seq_ = new AcknowledgedCounterGenerator(num_);

      if (name == Slice("open")) {
        method = &Benchmark::OpenBench;
        num_ /= 10000;
        if (num_ < 1) num_ = 1;
      } else if (name == Slice("LOAD")) {
        fprintf(stdout, "load\n");
        fresh_db = true;
        method = &Benchmark::YCSB_LOAD;
      } else if (name == Slice("A")) {
        fprintf(stdout, "A\n");
        fresh_db = false;
        key_chooser_ = new ScrambledZipfianGenerator(num_);
        operation_chooser_.AddValue(DBOperation::READ, 0.5);
        operation_chooser_.AddValue(DBOperation::UPDATE, 0.5);
        method = &Benchmark::YCSB_TXN;
      } else if (name == Slice("B")) {
        fprintf(stdout, "B\n");
        fresh_db = false;
        key_chooser_ = new ScrambledZipfianGenerator(num_);
        operation_chooser_.AddValue(DBOperation::READ, 0.95);
        operation_chooser_.AddValue(DBOperation::UPDATE, 0.05);
        method = &Benchmark::YCSB_TXN;
      } else if (name == Slice("C")) {
        fprintf(stdout, "C\n");
        fresh_db = false;
        key_chooser_ = new ScrambledZipfianGenerator(num_);
        operation_chooser_.AddValue(DBOperation::READ, 1);
        method = &Benchmark::YCSB_TXN;
      } else if (name == Slice("D")) {
        fprintf(stdout, "D\n");
        fresh_db = false;
        key_chooser_ = new SkewedLatestGenerator(*txn_insert_key_seq_);
        operation_chooser_.AddValue(DBOperation::READ, 0.95);
        operation_chooser_.AddValue(DBOperation::INSERT, 0.05);
        method = &Benchmark::YCSB_TXN;
      } else if (name == Slice("E")) {
        fprintf(stdout, "E\n");
        fresh_db = false;
        int op_count = ops_;
        double insert_proportion = 0.05;
        uint32_t min_scan_len = 1;
        uint32_t max_scan_len = 100; // workload E uses max_scan_len 100

        int new_keys = (int)(op_count * insert_proportion * 2);
        key_chooser_ = new ScrambledZipfianGenerator(num_ + new_keys);
        scan_length_ = new UniformGenerator(min_scan_len, max_scan_len);
        operation_chooser_.AddValue(DBOperation::SCAN, 0.95);
        operation_chooser_.AddValue(DBOperation::INSERT, 0.05);
        method = &Benchmark::YCSB_TXN;
      } else if (name == Slice("F")) {
        fprintf(stdout, "F\n");
        fresh_db = false;
        key_chooser_ = new ScrambledZipfianGenerator(num_);
        operation_chooser_.AddValue(DBOperation::READ, 0.5);
        operation_chooser_.AddValue(DBOperation::READMODIFYWRITE, 0.5); // request distribution : zipfian
        method = &Benchmark::YCSB_TXN;
      } else if (name == Slice("fillseq")) {
        fresh_db = true;
        method = &Benchmark::WriteSeq;
      } else if (name == Slice("fillbatch")) {
        fresh_db = true;
        entries_per_batch_ = 1000;
        method = &Benchmark::WriteSeq;
      } else if (name == Slice("fillrandom")) {
        fresh_db = true;
        method = &Benchmark::WriteRandom;
      } else if (name == Slice("overwrite")) {
        fresh_db = false;
        method = &Benchmark::WriteRandom;
      } else if (name == Slice("fillsync")) {
        fresh_db = true;
        num_ /= 1000;
        write_options_.sync = true;
        method = &Benchmark::WriteRandom;
      } else if (name == Slice("fill100K")) {
        fresh_db = true;
        num_ /= 1000;
        value_size_ = 100 * 1000;
        method = &Benchmark::WriteRandom;
      } else if (name == Slice("readseq")) {
        method = &Benchmark::ReadSequential;
      } else if (name == Slice("readreverse")) {
        method = &Benchmark::ReadReverse;
      } else if (name == Slice("readrandom")) {
        method = &Benchmark::ReadRandom;
      } else if (name == Slice("readmissing")) {
        method = &Benchmark::ReadMissing;
      } else if (name == Slice("seekrandom")) {
        method = &Benchmark::SeekRandom;
      } else if (name == Slice("readhot")) {
        method = &Benchmark::ReadHot;
      } else if (name == Slice("readrandomsmall")) {
        reads_ /= 1000;
        method = &Benchmark::ReadRandom;
      } else if (name == Slice("deleteseq")) {
        method = &Benchmark::DeleteSeq;
      } else if (name == Slice("deleterandom")) {
        method = &Benchmark::DeleteRandom;
      } else if (name == Slice("readwhilewriting")) {
        num_threads++;  // Add extra thread for writing
        method = &Benchmark::ReadWhileWriting;
      } else if (name == Slice("compact")) {
        method = &Benchmark::Compact;
      } else if (name == Slice("crc32c")) {
        method = &Benchmark::Crc32c;
      } else if (name == Slice("acquireload")) {
        method = &Benchmark::AcquireLoad;
      } else if (name == Slice("snappycomp")) {
        method = &Benchmark::SnappyCompress;
      } else if (name == Slice("snappyuncomp")) {
        method = &Benchmark::SnappyUncompress;
      } else if (name == Slice("heapprofile")) {
        HeapProfile();
      } else if (name == Slice("stats")) {
        PrintStats("leveldb.stats");
      } else if (name == Slice("sstables")) {
        PrintStats("leveldb.sstables");
      } else {
        if (name != Slice()) {  // No error message for empty name
          fprintf(stderr, "unknown benchmark '%s'\n", name.ToString().c_str());
        }
      }

      if (fresh_db) {
        if (FLAGS_use_existing_db) {
          fprintf(stdout, "%-12s : skipped (--use_existing_db is true)\n",
                  name.ToString().c_str());
          method = NULL;
        } else {
          delete db_;
          db_ = NULL;
          DestroyDB(FLAGS_db, Options());
          Open();
        }
      }

      if (method != NULL) {
        RunBenchmark(num_threads, name, method);
      }

      hm_manager_->ZoneUtilization();
      
      // uint64_t zone_num = hm_manager_->get_zone_num();
      // fprintf(stdout, "zone num : %lu\n", zone_num);

      // uint32_t used_zone = 0;
      // uint32_t space_amp = 0;

      
      // hm_manager_->get_zonenum_sa(&used_zone, &space_amp);
      // fprintf(stdout, "used zone num : %u, space amplification : %u\n", used_zone, space_amp);
      
      // uint64_t space_amp = hm_manager_->space_amplification(num_, 16, value_size_);
      // fprintf(stdout, "space amp : %lu\n", space_amp);
      
    }
  }

 private:
  struct ThreadArg {
    Benchmark* bm;
    SharedState* shared;
    ThreadState* thread;
    void (Benchmark::*method)(ThreadState*);
  };

  static void ThreadBody(void* v) {
    ThreadArg* arg = reinterpret_cast<ThreadArg*>(v);
    SharedState* shared = arg->shared;
    ThreadState* thread = arg->thread;
    {
      MutexLock l(&shared->mu);
      shared->num_initialized++;
      if (shared->num_initialized >= shared->total) {
        shared->cv.SignalAll();
      }
      while (!shared->start) {
        shared->cv.Wait();
      }
    }

    thread->stats.Start();
    (arg->bm->*(arg->method))(thread);
    thread->stats.Stop();

    {
      MutexLock l(&shared->mu);
      shared->num_done++;
      if (shared->num_done >= shared->total) {
        shared->cv.SignalAll(); // 마지막으로 실행을 끝낸 thread가 다른 스레드들 깨움
      }
    }
  }

  void RunBenchmark(int n, Slice name,
                    void (Benchmark::*method)(ThreadState*)) {
    SharedState shared;
    shared.total = n;
    shared.num_initialized = 0;
    shared.num_done = 0;
    shared.start = false;

    ThreadArg* arg = new ThreadArg[n];
    for (int i = 0; i < n; i++) {
      arg[i].bm = this;
      arg[i].method = method;
      arg[i].shared = &shared;
      arg[i].thread = new ThreadState(i);
      arg[i].thread->shared = &shared;
      g_env->StartThread(ThreadBody, &arg[i]);
    }

    shared.mu.Lock();
    while (shared.num_initialized < n) {
      shared.cv.Wait();
    }

    shared.start = true;
    shared.cv.SignalAll(); // 모든 thread들이 동시에 벤치마크 실행
    while (shared.num_done < n) {
      shared.cv.Wait(); // 모든 스레드들이 실행을 끝낸 후 여기서 깨어남
    }
    shared.mu.Unlock();

    for (int i = 1; i < n; i++) {
      arg[0].thread->stats.Merge(arg[i].thread->stats);
    }
    arg[0].thread->stats.Report(name);

    for (int i = 0; i < n; i++) {
      delete arg[i].thread;
    }
    delete[] arg;
    if (SLEEP_WAIT_FILL && name == Slice("fillrandom")) {
      while(db_->need_compaction()){
        sleep(1);
      }
    }
  }

  void Crc32c(ThreadState* thread) {
    // Checksum about 500MB of data total
    const int size = 4096;
    const char* label = "(4K per op)";
    std::string data(size, 'x');
    int64_t bytes = 0;
    uint32_t crc = 0;
    while (bytes < 500 * 1048576) {
      crc = crc32c::Value(data.data(), size);
      thread->stats.FinishedSingleOp();
      bytes += size;
    }
    // Print so result is not dead
    fprintf(stderr, "... crc=0x%x\r", static_cast<unsigned int>(crc));

    thread->stats.AddBytes(bytes);
    thread->stats.AddMessage(label);
  }

  void AcquireLoad(ThreadState* thread) {
    int dummy;
    port::AtomicPointer ap(&dummy);
    int count = 0;
    void *ptr = NULL;
    thread->stats.AddMessage("(each op is 1000 loads)");
    while (count < 100000) {
      for (int i = 0; i < 1000; i++) {
        ptr = ap.Acquire_Load();
      }
      count++;
      thread->stats.FinishedSingleOp();
    }
    if (ptr == NULL) exit(1); // Disable unused variable warning.
  }

  void SnappyCompress(ThreadState* thread) {
    RandomGenerator gen;
    Slice input = gen.Generate(Options().block_size);
    int64_t bytes = 0;
    int64_t produced = 0;
    bool ok = true;
    std::string compressed;
    while (ok && bytes < 1024 * 1048576) {  // Compress 1G
      ok = port::Snappy_Compress(input.data(), input.size(), &compressed);
      produced += compressed.size();
      bytes += input.size();
      thread->stats.FinishedSingleOp();
    }

    if (!ok) {
      thread->stats.AddMessage("(snappy failure)");
    } else {
      char buf[100];
      snprintf(buf, sizeof(buf), "(output: %.1f%%)",
               (produced * 100.0) / bytes);
      thread->stats.AddMessage(buf);
      thread->stats.AddBytes(bytes);
    }
  }

  void SnappyUncompress(ThreadState* thread) {
    RandomGenerator gen;
    Slice input = gen.Generate(Options().block_size);
    std::string compressed;
    bool ok = port::Snappy_Compress(input.data(), input.size(), &compressed);
    int64_t bytes = 0;
    char* uncompressed = new char[input.size()];
    while (ok && bytes < 1024 * 1048576) {  // Compress 1G
      ok =  port::Snappy_Uncompress(compressed.data(), compressed.size(),
                                    uncompressed);
      bytes += input.size();
      thread->stats.FinishedSingleOp();
    }
    delete[] uncompressed;

    if (!ok) {
      thread->stats.AddMessage("(snappy failure)");
    } else {
      thread->stats.AddBytes(bytes);
    }
  }

  void Open() {
    assert(db_ == NULL);
    Options options;
    options.env = g_env;
    options.create_if_missing = !FLAGS_use_existing_db;
    options.block_cache = cache_;
    options.write_buffer_size = FLAGS_write_buffer_size;
    options.max_file_size = FLAGS_max_file_size;
    options.block_size = FLAGS_block_size;
    options.max_open_files = FLAGS_open_files;
    options.filter_policy = filter_policy_;
    options.reuse_logs = FLAGS_reuse_logs;

    // no compression
    // CompressionType comType = kNoCompression;
    CompressionType comType = kSnappyCompression;
    options.compression = comType;
    if (comType == kNoCompression) {
      fprintf(stdout, "No Compression\n");
    } else {
      fprintf(stdout, "Snappy Compression\n");
    }
    
    Status s = DB::Open(options, FLAGS_db, &db_);
    if (!s.ok()) {
      fprintf(stderr, "open error: %s\n", s.ToString().c_str());
      exit(1);
    }
  }

  void OpenBench(ThreadState* thread) {
    for (int i = 0; i < num_; i++) {
      delete db_;
      Open();
      thread->stats.FinishedSingleOp();
    }
  }

  std::string BuildKeyName(uint64_t key_num) {
    if (!ordered_insert_) {
      key_num = Hash(key_num);
    }

    std::string prekey = "user";
    std::string value = std::to_string(key_num);
    int fill = std::max(0, zero_padding_ - static_cast<int>(value.size()));
    std::string pre_key("user");

    return prekey.append(fill, '0').append(value);
  }

  void BuildValues(std::vector<Field> &values) {
    for (int i = 0; i < field_count_; i++) {
      values.push_back(Field());
      Field& field = values.back();
      field.name.append(field_prefix_).append(std::to_string(i));
      uint64_t len = field_len_generator_->Next();
      field.value.reserve(len);
      RandomByteGenerator byte_generator;
      std::generate_n(std::back_inserter(field.value), len,
                      [&]() { return byte_generator.Next(); });
    }
  }

  void SerializeRow(const std::vector<Field>& values, std::string& data) {
    for (const Field& field : values) {
      uint32_t len = field.name.size();
      data.append(reinterpret_cast<char*>(&len), sizeof(uint32_t));
      data.append(field.name.data(), field.name.size());
      len = field.value.size();
      data.append(reinterpret_cast<char*>(&len), sizeof(uint32_t));
      data.append(field.value.data(), field.value.size());
    }
  }

  uint64_t NextTxnKeyNum() {
    uint64_t key;
    do {
      key = key_chooser_->Next();
    } while (key > txn_insert_key_seq_->Last());

    return key;
  }

  std::string NextFieldName() {
    return std::string(field_prefix_).append(std::to_string(field_chooser_->Next()));
  }

  void DeserializeRowFilter(std::vector<Field>& values,
                              const char* p, const char* lim,
                              const std::vector<std::string>& fields) {
    std::vector<std::string>::const_iterator filter_iter = fields.begin();
    while (p != lim && filter_iter != fields.end()) {
      assert(p < lim);
      uint32_t len = *reinterpret_cast<const uint32_t*>(p);
      p += sizeof(uint32_t);
      std::string field(p, static_cast<size_t>(len));
      p += len;
      len = *reinterpret_cast<const uint32_t*>(p);
      p += sizeof(uint32_t);
      std::string value(p, static_cast<size_t>(len));
      p += len;
      if (*filter_iter == field) {
        values.push_back({field, value});
        filter_iter++;
      }
    }
    assert(values.size() == fields.size());
  }

  void DeserializeRowFilter(std::vector<Field> &values, const std::string &data,
                              const std::vector<std::string> &fields) {
    const char *p = data.data();
    const char *l = p + data.size();
    DeserializeRowFilter(values, p, l, fields);
  }

  void DeserializeRow(std::vector<Field>& values, const char* p,
                                 const char* lim) {
    while (p != lim) {
      assert(p < lim);
      uint32_t len = *reinterpret_cast<const uint32_t*>(p);
      p += sizeof(uint32_t);
      std::string field(p, static_cast<size_t>(len));
      p += len;
      len = *reinterpret_cast<const uint32_t*>(p);
      p += sizeof(uint32_t);
      std::string value(p, static_cast<size_t>(len));
      p += len;
      values.push_back({field, value});
    }
  }

  void DeserializeRow(std::vector<Field>& values,
                                 const std::string& data) {
    const char* p = data.data();
    const char* lim = p + data.size();
    DeserializeRow(values, p, lim);
  }

  void BuildSingleValue(std::vector<Field>& values) {
    values.push_back(Field());
    Field& field = values.back();
    field.name.append(NextFieldName());
    uint64_t len = field_len_generator_->Next();
    field.value.reserve(len);
    RandomByteGenerator byte_generator;
    std::generate_n(std::back_inserter(field.value), len,
                    [&]() { return byte_generator.Next(); });
  }

  void DoYCSB(ThreadState *thread, int load) {

    if (load == 1) {
      // std::cout << "ycsb load\n";
      int i = 0;
      while (i < num_) {
        int64_t bytes = 0;
        std::string key = BuildKeyName(key_sequence_->Next());
        
        std::vector<Field> fields;
        BuildValues(fields);
        
        std::string data;
        SerializeRow(fields, data);
        
        Status s = db_->Put(write_options_, key, data);
        
        if (!s.ok()) {
          fprintf(stdout, "ycsb put error\n");
          exit(1);
          // ErrorExit();
        }
        thread->stats.FinishedSingleOp();
        thread->stats.AddBytes(data.size() + key.size());
        i++;   
      }
      
    } else {
      uint64_t i = 0;
      // int op_counts[5] = { 0 }; // 0 : update, 1 : read, 2 : insert, 3 : scan, 4 : read-modify-write

      while (i++ < ops_) { // 0 ~ num_ops
        DBOperation op;
        op = static_cast<DBOperation>(operation_chooser_.Next());
        
        if (op == DBOperation::READ) {
          // std::cout << "ycsb read\n";
          uint64_t key_num = NextTxnKeyNum();
          std::string key = BuildKeyName(key_num);
          std::vector<Field> result;
          ReadOptions options;

          if (!read_all_fields_) {
            std::vector<std::string> fields;
            fields.push_back(NextFieldName());

            std::string data;
            Status s = db_->Get(options, key, &data);
            if (!s.ok()) {
              fprintf(stdout, "ycsb read error\n");
              exit(1);
            }
            DeserializeRowFilter(result, data, fields);
            thread->stats.AddBytes(key.size() + data.size());
          } else {
            std::string data;
            Status s = db_->Get(options, key, &data);

            if (!s.ok()) {
              fprintf(stdout, "ycsb read error\n");
              exit(1);
            }

            DeserializeRow(result, data);
            thread->stats.AddBytes(key.size() + data.size());
          }

          thread->stats.FinishedSingleOp();
        } else if (op == DBOperation::UPDATE) {
          // std::cout << "ycsb update\n";
          uint64_t key_num = NextTxnKeyNum();
          std::string key = BuildKeyName(key_num);
          std::vector<Field> values;
          uint64_t bytes = 0;
          ReadOptions options;

          if (write_all_fields_) {
            BuildValues(values);
          } else {
            BuildSingleValue(values);
          }

          // db.Update()
          std::string data;
          Status s = db_->Get(options, key, &data);
          bytes += key.size() + data.size();

          if (!s.ok()) {

          }

          std::vector<Field> current_values;
          DeserializeRow(current_values, data);
          assert(current_values.size() == static_cast<size_t>(field_count_));

          for (Field &new_field : values) {
            bool found __attribute__ ((unused)) = false;
            for (Field &cur_field : current_values) {
              if (cur_field.name == new_field.name) {
                found = true;
                cur_field.value = new_field.value;
                break;
              }
            }
            assert(found);
          }

          data.clear();
          SerializeRow(current_values, data);
          s = db_->Put(write_options_, key, data);
          if (!s.ok()) {

          }
          bytes += key.size() + data.size();

          thread->stats.FinishedSingleOp();
          thread->stats.AddBytes(bytes);

        } else if (op == DBOperation::INSERT) {
          // std::cout << "ycsb insert\n";
          uint64_t key_num = txn_insert_key_seq_->Next();
          std::string key = BuildKeyName(key_num);
          std::vector<Field> values;
          uint64_t bytes = 0;
        
          BuildValues(values);

          std::string data;
          SerializeRow(values, data);

          Status s = db_->Put(write_options_, key, data);
          
          bytes += key.size() + data.size();

          if (!s.ok()) {
            fprintf(stdout, "put error\n");
          }

          txn_insert_key_seq_->Acknowledge(key_num);
          
          thread->stats.FinishedSingleOp();
          thread->stats.AddBytes(bytes);

        } else if (op == DBOperation::SCAN) {
          // std::cout << "ycsb scan\n";
          uint64_t key_num = NextTxnKeyNum();
          std::string key = BuildKeyName(key_num);
          int len = scan_length_->Next(); // 1 ~ 100
          std::vector<std::vector<Field>> result;
          uint64_t bytes = 0;
          ReadOptions options;

          if (!read_all_fields_) {
            std::vector<std::string> fields;
            
            fields.push_back(NextFieldName());
            
            Iterator *db_iter = db_->NewIterator(options);
            db_iter->Seek(key);
        
            for (int i = 0; db_iter->Valid() && i < len; i++) {
              std::string data = db_iter->value().ToString();
              result.push_back(std::vector<Field>());
              std::vector<Field> &values = result.back();
              bytes += db_iter->key().size() + db_iter->value().size();

              DeserializeRowFilter(values, data, fields);
              db_iter->Next();
            }

            delete db_iter;
          } else { // default
            Iterator* db_iter =
                db_->NewIterator(options);
            db_iter->Seek(key);

            for (int i = 0; db_iter->Valid() && i < len; i++) {
              std::string data = db_iter->value().ToString();
              result.push_back(std::vector<Field>());
              std::vector<Field>& values = result.back();
              bytes += db_iter->key().size() + db_iter->value().size();

              DeserializeRow(values, data);
              assert(values.size() == static_cast<size_t>(field_count_));
              
              db_iter->Next();
            }
            delete db_iter;
          }

          thread->stats.FinishedSingleOp();
          thread->stats.AddBytes(bytes);
        } else {
          // std::cout << "ycsb readmodifywrite\n";
          uint64_t key_num = NextTxnKeyNum();
          std::string key = BuildKeyName(key_num);
          std::vector<Field> result;
          ReadOptions options;

          if (!read_all_fields_) {
            std::vector<std::string> fields;
            fields.push_back(NextFieldName());

            std::string data;
            Status s = db_->Get(options, key, &data);

            if (!s.ok()) {

            }
            DeserializeRowFilter(result, data, fields);
            thread->stats.AddBytes(key.size() + data.size());
          }  else {
            std::string data;
            Status s = db_->Get(options, key, &data);

            if (!s.ok()) {

            }
            DeserializeRow(result, data);
            assert(result.size() == static_cast<size_t>(field_count_));

            thread->stats.AddBytes(key.size() + data.size());
          }

          std::vector<Field> values;
          if (write_all_fields_) {
            BuildValues(values);
          } else {
            BuildSingleValue(values);
          }

          std::string data;
          Status s = db_->Get(options, key, &data);
          
          if (!s.ok()) {

          }

          std::vector<Field> current_values;
          DeserializeRow(current_values, data);
          assert(current_values.size() == static_cast<size_t>(field_count_));

          for (Field &new_field : values) {
            bool found __attribute__ ((unused)) = false;
            for (Field &cur_field : current_values) {
              if (cur_field.name == new_field.name) {
                found = true;
                cur_field.value = new_field.value;
                break;
              }
            }
            assert(found);
          }

          data.clear();
          SerializeRow(current_values, data);
          s = db_->Put(write_options_, key, data);
          if (!s.ok()) {

          } 

          thread->stats.FinishedSingleOp();
          thread->stats.AddBytes(key.size() + data.size());
        }
      }
      // std::cout << "total bytes : " << total_bytes << "\n";
      // thread->stats.AddBytes(total_bytes);
      
      // // count operations
      // for (int i = 0; i < 5; i++) {
      //   switch(i) {
      //     case 0:
      //       std::cout << "\n read : " << op_counts[i] << "\n";
      //       break;
      //     case 1:
      //       std::cout << "update : " << op_counts[i] << "\n";
      //       break;
      //     case 2:
      //       std::cout << "insert : " << op_counts[i] << "\n";
      //       break;
      //     case 3:
      //       std::cout << "scan : " << op_counts[i] << "\n";
      //       break;
      //     case 4:
      //       std::cout << "read modify write : " << op_counts[i] << "\n";
      //       break;
      //   }

      // }

    }

  }

  void YCSB_LOAD(ThreadState *thread) {
    // fprintf(stdout, "ycsb load\n");
    DoYCSB(thread, 1);
  }

  void YCSB_TXN(ThreadState *thread) {
    DoYCSB(thread, 0);
  }
  
  void WriteSeq(ThreadState* thread) {
    DoWrite(thread, true);
  }

  void WriteRandom(ThreadState* thread) {
    DoWrite(thread, false);
  }

  void DoWrite(ThreadState* thread, bool seq) {
    // fprintf(stdout, "DoWrite\n");
    if (num_ != FLAGS_num) {
      char msg[100];
      snprintf(msg, sizeof(msg), "(%d ops)", num_);
      thread->stats.AddMessage(msg);
    }

    RandomGenerator gen;
    WriteBatch batch;
    Status s;
    int64_t bytes = 0;
    int64_t last_bytes = 0;
    double last_time = thread->stats.start_;
    double now = 0;
    int ret=0;
    for (int i = 0; i < num_; i += entries_per_batch_) {
      batch.Clear();
      for (int j = 0; j < entries_per_batch_; j++) {
        const int k = seq ? i+j : ((thread->rand.Next() % FLAGS_num) + (thread->tid * num_));
        char key[100];
        // snprintf(key, sizeof(key), "%016d", k);
        snprintf(key, sizeof(key), "%020d", k); // 20 bytes key
        batch.Put(key, gen.Generate(value_size_));
        bytes += value_size_ + strlen(key);
        // fprintf(stdout, "key size : %ld, value size : %d\n", strlen(key), value_size_);
        thread->stats.FinishedSingleOp();
      }
      s = db_->Write(write_options_, &batch);
      if (!s.ok()) {
        fprintf(stderr, "put error: %s\n", s.ToString().c_str());
        exit(1);
      }
      if ((i + 1)% 250000 == 0 || (i+1)==num_) {
        now = g_env->NowMicros();
        double mytime = (now - thread->stats.start_) * 1e-6;
        double elapsed = (now - last_time) * 1e-6;
        int64_t ebytes = bytes - last_bytes;
        last_time = now;
        last_bytes = bytes;
        MyLog5("i=%d, every 250000 ops(1G), speed=%2.1lfMB/s time=%.2f s run_time:%.2f s size:%.2f MB\n", i+1, (ebytes / 1048576.0) / elapsed,elapsed,mytime,bytes/1048576.0);
        std::string status;
        db_->GetProperty("leveldb.stats",&status);
        hm_manager_->get_my_info(i+1);
        MyLog6("%s",status.c_str());
        double log_time = db_->get_log_write_time();
        MyLog6("\nLog write time:%.2f s\n",log_time * 1e-6);
      }
    }
    hm_manager_->get_all_info();

    thread->stats.AddBytes(bytes);
  }

  void ReadSequential(ThreadState* thread) {
    Iterator* iter = db_->NewIterator(ReadOptions());
    int i = 0;
    int64_t bytes = 0;
    int read_num = FLAGS_reads_seq;
    for (iter->SeekToFirst(); i < read_num && iter->Valid(); iter->Next()) {
      bytes += iter->key().size() + iter->value().size();
      thread->stats.FinishedSingleOp();
      ++i;
    }
    delete iter;
    thread->stats.AddBytes(bytes);
    char msg[100];
    snprintf(msg, sizeof(msg), " (%d)", read_num);
    thread->stats.AddMessage(msg);
  }

  void ReadReverse(ThreadState* thread) {
    Iterator* iter = db_->NewIterator(ReadOptions());
    int i = 0;
    int64_t bytes = 0;
    for (iter->SeekToLast(); i < reads_ && iter->Valid(); iter->Prev()) {
      bytes += iter->key().size() + iter->value().size();
      thread->stats.FinishedSingleOp();
      ++i;
    }
    delete iter;
    thread->stats.AddBytes(bytes);
  }

  void ReadRandom(ThreadState* thread) {
    ReadOptions options;
    std::string value;
    int found = 0;
    for (int i = 0; i < reads_; i++) {
      char key[100];
      const int k = thread->rand.Next() % FLAGS_num;
      // snprintf(key, sizeof(key), "%016d", k);
      snprintf(key, sizeof(key), "%020d", k);
      if (db_->Get(options, key, &value).ok()) {
        found++;
      }
      thread->stats.FinishedSingleOp();
    }
    char msg[100];
    snprintf(msg, sizeof(msg), "(%d of %d found)", found, num_);
    thread->stats.AddMessage(msg);
  }

  void ReadMissing(ThreadState* thread) {
    ReadOptions options;
    std::string value;
    for (int i = 0; i < reads_; i++) {
      char key[100];
      const int k = thread->rand.Next() % FLAGS_num;
      // snprintf(key, sizeof(key), "%016d.", k);
      snprintf(key, sizeof(key), "%020d.", k);
      db_->Get(options, key, &value);
      thread->stats.FinishedSingleOp();
    }
  }

  void ReadHot(ThreadState* thread) {
    ReadOptions options;
    std::string value;
    const int range = (FLAGS_num + 99) / 100;
    for (int i = 0; i < reads_; i++) {
      char key[100];
      const int k = thread->rand.Next() % range;
      // snprintf(key, sizeof(key), "%016d", k);
      snprintf(key, sizeof(key), "%020d", k);
      db_->Get(options, key, &value);
      thread->stats.FinishedSingleOp();
    }
  }

  void SeekRandom(ThreadState* thread) {
    ReadOptions options;
    int found = 0;
    for (int i = 0; i < reads_; i++) {
      Iterator* iter = db_->NewIterator(options);
      char key[100];
      const int k = thread->rand.Next() % FLAGS_num;
      // snprintf(key, sizeof(key), "%016d", k);
      snprintf(key, sizeof(key), "%020d", k);
      iter->Seek(key);
      if (iter->Valid() && iter->key() == key) found++;
      delete iter;
      thread->stats.FinishedSingleOp();
    }
    char msg[100];
    snprintf(msg, sizeof(msg), "(%d of %d found)", found, num_);
    thread->stats.AddMessage(msg);
  }

  void DoDelete(ThreadState* thread, bool seq) {
    RandomGenerator gen;
    WriteBatch batch;
    Status s;
    for (int i = 0; i < num_; i += entries_per_batch_) {
      batch.Clear();
      for (int j = 0; j < entries_per_batch_; j++) {
        const int k = seq ? i+j : (thread->rand.Next() % FLAGS_num);
        char key[100];
        // snprintf(key, sizeof(key), "%016d", k);
        snprintf(key, sizeof(key), "%020d", k);
        batch.Delete(key);
        thread->stats.FinishedSingleOp();
      }
      s = db_->Write(write_options_, &batch);
      if (!s.ok()) {
        fprintf(stderr, "del error: %s\n", s.ToString().c_str());
        exit(1);
      }
    }
  }

  void DeleteSeq(ThreadState* thread) {
    DoDelete(thread, true);
  }

  void DeleteRandom(ThreadState* thread) {
    DoDelete(thread, false);
  }

  void ReadWhileWriting(ThreadState* thread) {
    if (thread->tid > 0) {
      ReadRandom(thread);
    } else {
      // Special thread that keeps writing until other threads are done.
      RandomGenerator gen;
      while (true) {
        {
          MutexLock l(&thread->shared->mu);
          if (thread->shared->num_done + 1 >= thread->shared->num_initialized) {
            // Other threads have finished
            break;
          }
        }

        const int k = thread->rand.Next() % FLAGS_num;
        char key[100];
        // snprintf(key, sizeof(key), "%016d", k);
        snprintf(key, sizeof(key), "%020d", k);
        Status s = db_->Put(write_options_, key, gen.Generate(value_size_));
        if (!s.ok()) {
          fprintf(stderr, "put error: %s\n", s.ToString().c_str());
          exit(1);
        }
      }

      // Do not count any of the preceding work/delay in stats.
      thread->stats.Start();
    }
  }

  void Compact(ThreadState* thread) {
    db_->CompactRange(NULL, NULL);
  }

  void PrintStats(const char* key) {
    std::string stats;
    if (!db_->GetProperty(key, &stats)) {
      stats = "(failed)";
    }
    fprintf(stdout, "\n%s\n", stats.c_str());
    double log_time = db_->get_log_write_time();
    fprintf(stdout,"Log write time:%.2f s\n",log_time * 1e-6);
  }

  static void WriteToFile(void* arg, const char* buf, int n) {
    reinterpret_cast<WritableFile*>(arg)->Append(Slice(buf, n));
  }

  void HeapProfile() {
    char fname[100];
    snprintf(fname, sizeof(fname), "%s/heap-%04d", FLAGS_db, ++heap_counter_);
    WritableFile* file;
    Status s = g_env->NewWritableFile(fname, &file);
    if (!s.ok()) {
      fprintf(stderr, "%s\n", s.ToString().c_str());
      return;
    }
    bool ok = port::GetHeapProfile(WriteToFile, file);
    delete file;
    if (!ok) {
      fprintf(stderr, "heap profiling not supported\n");
      g_env->DeleteFile(fname);
    }
  }
};

}  // namespace leveldb

int main(int argc, char** argv) {
  FLAGS_write_buffer_size = leveldb::Options().write_buffer_size;
  FLAGS_max_file_size = leveldb::Options().max_file_size;
  FLAGS_block_size = leveldb::Options().block_size;
  FLAGS_open_files = leveldb::Options().max_open_files;
  std::string default_db_path;

  for (int i = 1; i < argc; i++) {
    double d;
    int n;
    char junk;
    if (leveldb::Slice(argv[i]).starts_with("--benchmarks=")) {
      FLAGS_benchmarks = argv[i] + strlen("--benchmarks=");
    } else if (sscanf(argv[i], "--compression_ratio=%lf%c", &d, &junk) == 1) {
      FLAGS_compression_ratio = d;
    } else if (sscanf(argv[i], "--histogram=%d%c", &n, &junk) == 1 &&
               (n == 0 || n == 1)) {
      FLAGS_histogram = n;
    } else if (sscanf(argv[i], "--use_existing_db=%d%c", &n, &junk) == 1 &&
               (n == 0 || n == 1)) {
      FLAGS_use_existing_db = n;
    } else if (sscanf(argv[i], "--reuse_logs=%d%c", &n, &junk) == 1 &&
               (n == 0 || n == 1)) {
      FLAGS_reuse_logs = n;
    } else if (sscanf(argv[i], "--num=%d%c", &n, &junk) == 1) {
      FLAGS_num = n;
    } else if (sscanf(argv[i], "--ops=%d%c", &n, &junk) == 1) { // to support YCSB
      FLAGS_ops = n;
    } else if (sscanf(argv[i], "--reads=%d%c", &n, &junk) == 1) {
      FLAGS_reads = n;
    } else if (sscanf(argv[i], "--reads_seq=%d%c", &n, &junk) == 1) {
      FLAGS_reads_seq = n;
    } else if (sscanf(argv[i], "--threads=%d%c", &n, &junk) == 1) {
      FLAGS_threads = n;
    } else if (sscanf(argv[i], "--value_size=%d%c", &n, &junk) == 1) {
      FLAGS_value_size = n;
    } else if (sscanf(argv[i], "--write_buffer_size=%d%c", &n, &junk) == 1) {
      FLAGS_write_buffer_size = n;
    } else if (sscanf(argv[i], "--max_file_size=%d%c", &n, &junk) == 1) {
      FLAGS_max_file_size = n;
    } else if (sscanf(argv[i], "--block_size=%d%c", &n, &junk) == 1) {
      FLAGS_block_size = n;
    } else if (sscanf(argv[i], "--cache_size=%d%c", &n, &junk) == 1) {
      FLAGS_cache_size = n;
    } else if (sscanf(argv[i], "--bloom_bits=%d%c", &n, &junk) == 1) {
      FLAGS_bloom_bits = n;
    } else if (sscanf(argv[i], "--open_files=%d%c", &n, &junk) == 1) {
      FLAGS_open_files = n;
    } else if (strncmp(argv[i], "--db=", 5) == 0) {
      FLAGS_db = argv[i] + 5;
    } else {
      fprintf(stderr, "Invalid flag '%s'\n", argv[i]);
      exit(1);
    }
  }

  leveldb::g_env = leveldb::Env::Default();

  // Choose a location for the test database if none given with --db=<path>
  if (FLAGS_db == NULL) {
      leveldb::g_env->GetTestDirectory(&default_db_path);
      default_db_path += "/dbbench";
      FLAGS_db = default_db_path.c_str();
  }

  leveldb::Benchmark benchmark;
  benchmark.Run();
  return 0;
}

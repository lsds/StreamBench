#ifndef READ_H_
#define READ_H_

#include <string>
#include <boost/progress.hpp> /* progress bar */

#include "config.h"
#include "log.h"

#include "Bounded.h"
#include "core/Transforms.h"

using namespace std;

/**
 * A {@link PTransform} for reading from a {@link Source}.
 *
 * <p>Usage example:
 * <pre>
 * Pipeline p = Pipeline.create();
 * p.apply(Read.from(new MySource().withFoo("foo").withBar("bar"))
 *             .named("foobar"));
 * </pre>
 */

template <class T>
class Bounded;

template <class T>
class UnBounded;

template <class T>
class Read {
public:

	/**
	 * Returns a new {@code Read} {@code PTransform} builder with the given name.
	 */
	class Builder;
	static Builder* named(string name) {
		return new Builder(name);
	}

	static Bounded<T>* from(BoundedSource<T>* source) {
		return new Bounded<T>("", source);
	}

	static Bounded<T>* from(BoundedSource<T>* source, string name) {
		return new Bounded<T>(name, source);
	}

	/**
	 * Helper class for building {@code Read} transforms.
	 * xzl: never exposed outside??
	 */
	class Builder {
	private:
		string _name;

	public:
		/**
		 * Returns a new {@code Read.Bounded} {@code PTransform} reading from the given
		 * {@code BoundedSource}.
		 */
		Bounded<T>* from(BoundedSource<T>* source) {
			return new Bounded<T>(_name, source);
		}

		Builder(string name) :	_name(name) { }

		/**
		 * Returns a new {@code Read.Unbounded} {@code PTransform} reading from the given
		 * {@code UnboundedSource}.
		 */
//		UnBounded* from(UnboundedSource* source) {
//			return new Unbounded(_name, source);
//		}
	};
};


/* PTransform that reads from BoundedSource.
 * T: the element type of the source
 *  */
template<class T>
class Bounded: public PTransform {

public:
	Bounded(string name, BoundedSource<T>* source) :
			PTransform(name), _source(source) {
	}

	BoundedSource<T>* _source;

	/**
	 * Returns a new {@code Bounded} {@code PTransform} that's like this one but
	 * has the given name.
	 *
	 * <p>Does not modify this object.
	 */
	Bounded* named(string name) {
		return new Bounded(name, _source);
	}

	BoundedSource<T>* getSource() {
		return _source;
	}

	string getKindString() {
		return "Bounded";
	}

	// source, no-op
	virtual ptime RefreshWatermark(ptime wm) override {
		return wm;
	}

};

/* DEPRECATED by UnboundedInMem() */
template<class T>
class UnboundedPseudo : public PTransform {
public:

	int interval_ms;  /* the time difference between consecutive bundles */
	int session_gap_ms; /* gap between "bursts" of bundles. test session windows */

	UnboundedPseudo (string name, int interval_ms = 100, int session_gap_ms = 0)
			: PTransform(name), interval_ms(interval_ms),
			  session_gap_ms(session_gap_ms) { }

	// source, no-op
	virtual ptime RefreshWatermark(ptime wm) override {
		return wm;
	}

};

#include <sys/mman.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <string.h>
#include <ctype.h>
#include <numa.h>

#if 0 /* don't introduce the dependency as kaskade will create tp */
/* numa aware */
#include "utilities/threading.hh"
using namespace Kaskade;
#endif

/* the data are preloaded in memory. and the evaluator just replay the data */

/* will produce bundles of Records of @T in @BundleT */
template<class T,  								/* record data type */
		template<class> class BundleT		/* bundle type */
>
class UnboundedInMem; /* generic impl not available */


#if 0
template<template<class> class BundleT>   /* can't use default argument = RecordBitmapBundle<string_range> */
class UnboundedInMem<string_range, BundleT> : public PTransform {

public:
  const char * input_fname;

  /* these are event time, irrelevant to the engine processing timing */
  int punc_interval_ms;
  int num_records;  /* # records between two puncs */
  int interval_ms;  /* the ts difference between consecutive bundles.*/
  int session_gap_ms; /* gap between "bursts" of bundles. for testing session windows */

  vector<char const *> buffers; /* buffers, one for each NUMA node */
  uint64_t buffer_size = 0;

  UnboundedInMem (string name, const char *input_fname,
      int interval_ms = 100, int session_gap_ms = 0)
    : PTransform(name), input_fname(input_fname), interval_ms(interval_ms),
      session_gap_ms(session_gap_ms),
      byte_counter_(0), record_counter_(0)
  {

    int fd;
    struct stat finfo;
    int num_nodes = numa_num_configured_nodes();

    CHECK_ERROR((fd = open(input_fname, O_RDONLY)) < 0);
    CHECK_ERROR(fstat(fd, &finfo) < 0);

//    NumaThreadPool& pool = NumaThreadPool::instance();
    buffer_size = finfo.st_size;

//    EE("to fill %lu buffers (size %lu) with contents from file %s",
//        buffers.size(), buffer_size, input_fname);

    /* get per-node buffers and fill the buffers with the file contents */
    for (int i = 0; i < num_nodes; i++) {
//        char * p = (char *)pool.allocator(i).alloc(buffer_size);
    	char *p = (char *)numa_alloc_onnode(buffer_size, i);
			assert(p);

			uint64_t r = 0;
			while (r < buffer_size) {
				auto ret = pread(fd, p + r, buffer_size, r);
				if (ret == 0 || ret == -1) {
					perror("read failure");
					abort();
				} else {
					r += ret;
					cout << "read " << ret << " bytes\n";
					printf("read one buffer. %2f \n", (double)r/buffer_size);
				}
			}
			assert(r == buffer_size);

			buffers.push_back(p);
    }

    EE("filled %lu buffers (size %lu) with contents from file %s",
        buffers.size(), buffer_size, input_fname);
  }

  // source, no-op
  virtual ptime RefreshWatermark(ptime wm) override {
    return wm;
  }


  /* internal accounting  -- to be updated by the evaluator*/
  atomic<unsigned long> byte_counter_, record_counter_;

  bool ReportStatistics(PTransform::Statstics* stat) override {

    /* internal accounting */
     unsigned long total_records =
    		 record_counter_.load(std::memory_order_relaxed);
     unsigned long total_bytes =
    		 byte_counter_.load(std::memory_order_relaxed);

     /* last time we report */
     static unsigned long last_bytes = 0, last_records = 0;
     static ptime last_check, start_time;
     static int once = 1;

   	ptime now = boost::posix_time::microsec_clock::local_time();

   	if (once) {
   		once = 0;
   		last_check = now;
   		start_time = now;
   		last_records = total_records;
   		return false;
   	}

   	boost::posix_time::time_duration diff = now - last_check;

   	{
   		double interval_sec = (double) diff.total_milliseconds() / 1000;
   		double total_sec = (double) (now - start_time).total_milliseconds() / 1000;

   		stat->name = this->name.c_str();
   		stat->mbps = (double) total_bytes / total_sec;
   		stat->mrps = (double) total_records / total_sec;

   		stat->lmbps = (double) (total_bytes - last_bytes) / interval_sec;
   		stat->lmrps = (double) (total_records - last_records) / interval_sec;

   		last_check = now;
   		last_bytes = total_bytes;
   		last_records = total_records;
   	}

   	return true;
  }

  void ExecEvaluator(int nodeid, EvaluationBundleContext *c, shared_ptr<BundleBase>) override;

//  void ExecEvaluator(int nodeid, EvaluationBundleContext *c) override {
//  	/* instantiate an evaluator */
//  	UnboundedInMemEvaluator<string_range> eval(nodeid);
//		eval.evaluate(this, c);
//  }

};
#endif

#include <iostream>
#include <fstream>      // std::ifstream
#include <json.hpp>  /* from the modern c++ json parser, /ssd/local/include */
using json = nlohmann::json;

enum text_mode {
	TXT_PLAIN, /* will load into fixed-size string range */
	TXT_TWEETS,
	TXT_LINES,
};

/* For tweet.
 * prefill the buffer with Record<string_range> */
template<template<class> class BundleT>   /* can't use default argument = RecordBitmapBundle<string_range> */
class UnboundedInMem<string_range, BundleT> : public PTransform {

	using T = string_range;

public:

	const char * input_fname;

	/* these are event time, irrelevant to the engine processing timing */
	const int punc_interval_ms = 1000;
	const unsigned long records_per_interval;  /* # records between two puncs */
	uint64_t string_len; /* the length covered by each record */
	const int session_gap_ms; /* gap between "bursts" of bundles. for testing session windows */
	const uint64_t target_tput; /* in record/sec */

	/* array of prefilled buffers of records, one for each NUMA node */
//  vector<vector<Record<T>>> records;
	vector<Record<T> *> record_buffers;
	uint64_t buffer_size_records = 0;
	/* # of (duplicated) output streams */
	const int num_outputs;

private:
	vector<char const *> buffers; /* buffers, one for each NUMA node */
	uint64_t buffer_size = 0;

public:
	UnboundedInMem (string name, const char *input_fname, unsigned long rpi, uint64_t tt, uint64_t record_size, int session_gap_ms, text_mode mode = TXT_PLAIN, int num_outputs = 1)
			: PTransform(name), input_fname(input_fname), records_per_interval(rpi), string_len(record_size), session_gap_ms(session_gap_ms), target_tput(tt), num_outputs(num_outputs), byte_counter_(0), record_counter_(0)
	{
		int fd;
		struct stat finfo;
		int num_nodes;

		if (mode == TXT_TWEETS) {
			UnboundedInMemTweets(name, input_fname, rpi, tt, record_size,
								 session_gap_ms);
			return;
		} else if (mode == TXT_LINES) {
			UnboundedInMemTextLines(name, input_fname, rpi, tt, record_size,
									session_gap_ms);
			return;
		}

		num_nodes = numa_num_configured_nodes();

		CHECK_ERROR((fd = open(input_fname, O_RDONLY)) < 0);
		CHECK_ERROR(fstat(fd, &finfo) < 0);

		buffer_size = std::max(CONFIG_MIN_PERNODE_BUFFER_SIZE_MB * 1024 *1024,
							   CONFIG_MIN_EPOCHS_PERNODE_BUFFER * records_per_interval * string_len);

		/* sanity check: file long enough for the buffer? */
		if ((int64_t)buffer_size > finfo.st_size) {
			EE("input data not enough. need %.2f MB. has %.2f MB",
			   (double) buffer_size / 1024 / 1024, (double) finfo.st_size / 1024 / 1024);
			abort();
		}

		buffer_size_records = buffer_size / string_len;
		xzl_assert(buffer_size_records > 0);

		char *rpath = realpath(input_fname, NULL);
		xzl_bug_on(!rpath);

		printf("---- source configuration ---- \n");
		printf("source file: %s (specified as: %s)\n", rpath, input_fname);
		free(rpath);
		printf("source file size: %.2f MB\n", (double)finfo.st_size/1024/1024);
		printf("buffer size: %.2f MB\n", (double)buffer_size/1024/1024);
		printf("%10s %10s %10s %10s %10s %10s %10s %10s\n",
			   "#nodes:", "KRec/nodebuf", "MB", "epoch/ms", "KRec/epoch", "MB/epoc",
			   "target:KRec/S", "RecSize" );
		printf("%10d %10lu %10lu %10d %10lu %10lu %10lu %10lu\n",
			   num_nodes, buffer_size_records/1000, buffer_size/1024/1024,
			   punc_interval_ms, records_per_interval/1000,
			   records_per_interval * string_len /1024/1024,
			   target_tput/1000, string_len);

		/* get per-node buffers and fill the buffers with the file contents */
		for (int i = 0; i < num_nodes; i++) {

			printf("node %d: loading buffer %.2f MB\n", i, (double)buffer_size/1024/1024);

			boost::progress_display show_progress(buffer_size);

			char *p = (char *)numa_alloc_onnode(buffer_size, i);
			assert(p);

			uint64_t r = 0;
			while (r < buffer_size) {
				auto ret = pread(fd, p + r, buffer_size, r);
				if (ret == 0 || ret == -1) {
					perror("read failure");
					abort();
				} else {
					r += ret;
					show_progress += ret;
//					cout << "read " << ret << " bytes\n";
//					printf("read one buffer. %.2f \n", (double)r/buffer_size);
				}
			}
			assert(r == buffer_size);

			buffers.push_back(p);
		}

		/* fill the buffers of records */

		for (int i = 0; i < num_nodes; i++) {
			Record<T> * record_buffer =
					(Record<T> *) numa_alloc_onnode(sizeof(Record<T>) * buffer_size_records,
													i);

			xzl_assert(record_buffer);

			for (unsigned int j = 0; j < buffer_size_records; j++) {
				record_buffer[j].data.data = buffers[i] + j * string_len;
				record_buffer[j].data.len = string_len;
				/* record_buffer[j].ts will be filled by eval */
			}

			record_buffers.push_back(record_buffer);
		}

//    EE("filled %lu buffers, each with %lu records,"
//    		"each covering size %lu MB from file %s.",
//        buffers.size(), buffer_size_records, buffer_size/1024/1024, input_fname);
	}

	/* ingest tweets json files: extract the ts and text body (as string_range)
     * dispatched from ctor */
	void UnboundedInMemTweets(string name, const char *input_fname, unsigned long rpi, uint64_t tt, uint64_t record_size, int session_gap_ms)
	{
		int fd;
		struct stat finfo;
		int num_nodes;

		num_nodes = numa_num_configured_nodes();

		CHECK_ERROR((fd = open(input_fname, O_RDONLY)) < 0);
		CHECK_ERROR(fstat(fd, &finfo) < 0);

		xzl_assert(string_len >= 140); /* must have enough data */

		buffer_size = std::max(CONFIG_MIN_PERNODE_BUFFER_SIZE_MB * 1024 *1024,
							   CONFIG_MIN_EPOCHS_PERNODE_BUFFER * records_per_interval * string_len);

		/* sanity check: file long enough for the buffer? */
		if ((int64_t) buffer_size > finfo.st_size) {
			EE("input data not enough. need %.2f MB. has %.2f MB",
			   (double ) buffer_size / 1024 / 1024,
			   (double ) finfo.st_size / 1024 / 1024);
			abort();
		}

		buffer_size_records = buffer_size / string_len;
		xzl_assert(buffer_size_records > 0);

		printf("---- source configuration ---- \n");
		printf("source file: %s\n", input_fname);
		printf("source file size: %.2f MB\n", (double) finfo.st_size / 1024 / 1024);
		printf("buffer size: %.2f MB\n", (double) buffer_size / 1024 / 1024);
		printf("%10s %10s %10s %10s %10s %10s %10s %10s\n", "#nodes:", "KRec/nodebuf", "MB",
			   "epoch/ms", "KRec/epoch", "MB/epoc", "target:KRec/S", "RecSize");
		printf("%10d %10lu %10lu %10d %10lu %10lu %10lu %10lu\n", num_nodes,
			   buffer_size_records / 1000, buffer_size / 1024 / 1024, punc_interval_ms,
			   records_per_interval / 1000,
			   records_per_interval * string_len / 1024 / 1024, target_tput / 1000,
			   string_len);

		/* get per-node buffers and fill the buffers with the file contents.
		 * assemble the corresponding record at the same time */

		std::ifstream infile(input_fname);

		for (int i = 0; i < num_nodes; i++) {

			printf("node %d: loading buffer %.2f MB\n", i,
				   (double) buffer_size / 1024 / 1024);

			boost::progress_display show_progress(buffer_size_records);

			char *p = (char *) numa_alloc_onnode(buffer_size, i); /* txt buffer */
			xzl_bug_on(!p);
			buffers.push_back(p);

			Record<T> * record_buffer = (Record<T> *) numa_alloc_onnode(
					sizeof(Record<T> ) * buffer_size_records, i); /* record buffer */
			xzl_bug_on(!record_buffer);
			record_buffers.push_back(record_buffer);

			/* load json object */
			std::string line;

			uint64_t r = 0, k = 0 /* record id */;
			int bad_recs = 0;
			while (std::getline(infile, line)) {
				auto j = json::parse(line);

//				std::string text = j["text"];
//				auto s = j["text"];
//				if (!s) {
				/* can't do direct assignment, must do this */
				if (j.count("text") != 1) {

					/* we may see json lines like:
					 * {"limit":{"track":59,"timestamp_ms":"1482893264899"}}
					 */
#if 0
					cout << "bad line:";
					cout << line << endl; // debugging
#endif
					bad_recs ++;
					continue;
				}
				std::string text = j["text"];

				auto text_len = text.size();
				xzl_bug_on(text_len == 0);

				auto ret = text.copy(p + r,
//						std::min(text_len, buffer_size - r), /* max to copy */
									 buffer_size - r, /* max to copy */
									 0 /* offset in string*/);
				xzl_bug_on(text_len != ret);

				record_buffer[k].data.data = p + r;
				record_buffer[k].data.len = text_len;

				/* todo : extract & fill record ts */

				k++;
				r += text_len;
				show_progress += 1;

				if (k == buffer_size_records) /* enough records. done */
					break;
			}
			printf("bad json objs = %d\n", bad_recs);
			xzl_bug_on(k != buffer_size_records);

		}  // numa nodes
	}

	/* ingest a file as text lines: all lines are copied into a big buffer
     * and each line is string_range */
	void UnboundedInMemTextLines(string name, const char *input_fname, unsigned long rpi, uint64_t tt, uint64_t record_size, int session_gap_ms)
	{
		int fd;
		struct stat finfo;
		int num_nodes;

		num_nodes = numa_num_configured_nodes();

		CHECK_ERROR((fd = open(input_fname, O_RDONLY)) < 0);
		CHECK_ERROR(fstat(fd, &finfo) < 0);

		xzl_assert(string_len >= 200); /* must have enough data */

		buffer_size = std::max(CONFIG_MIN_PERNODE_BUFFER_SIZE_MB * 1024 *1024,
							   CONFIG_MIN_EPOCHS_PERNODE_BUFFER * records_per_interval * string_len);

		/* sanity check: file long enough for the buffer? */
		if ((int64_t) buffer_size > finfo.st_size) {
			EE("input data not enough. need %.2f MB. has %.2f MB",
			   (double ) buffer_size / 1024 / 1024,
			   (double ) finfo.st_size / 1024 / 1024);
			abort();
		}

		buffer_size_records = buffer_size / string_len;
		xzl_assert(buffer_size_records > 0);

		printf("---- source configuration ---- \n");
		printf("source file: %s\n", input_fname);
		printf("source file size: %.2f MB\n", (double) finfo.st_size / 1024 / 1024);
		printf("buffer size: %.2f MB\n", (double) buffer_size / 1024 / 1024);
		printf("%10s %10s %10s %10s %10s %10s %10s %10s\n", "#nodes:", "KRec/nodebuf", "MB",
			   "epoch/ms", "KRec/epoch", "MB/epoc", "target:KRec/S", "RecSize");
		printf("%10d %10lu %10lu %10d %10lu %10lu %10lu %10lu\n", num_nodes,
			   buffer_size_records / 1000, buffer_size / 1024 / 1024, punc_interval_ms,
			   records_per_interval / 1000,
			   records_per_interval * string_len / 1024 / 1024, target_tput / 1000,
			   string_len);

		/* get per-node buffers and fill the buffers with the file contents.
		 * assemble the corresponding record at the same time */

		std::ifstream infile(input_fname);

		for (int i = 0; i < num_nodes; i++) {

			printf("node %d: loading buffer %.2f MB\n", i,
				   (double) buffer_size / 1024 / 1024);

			boost::progress_display show_progress(buffer_size_records);

			char *p = (char *) numa_alloc_onnode(buffer_size, i); /* txt buffer */
			xzl_bug_on(!p);
			buffers.push_back(p);

			Record<T> * record_buffer = (Record<T> *) numa_alloc_onnode(
					sizeof(Record<T> ) * buffer_size_records, i); /* record buffer */
			xzl_bug_on(!record_buffer);
			record_buffers.push_back(record_buffer);

			std::string line;
			uint64_t r = 0, k = 0 /* record id */;
			while (std::getline(infile, line)) {

				auto text_len = line.size();
				xzl_bug_on(text_len == 0);

				auto ret = line.copy(p + r,
//						std::min(text_len, buffer_size - r), /* max to copy */
									 buffer_size - r, /* max to copy */
									 0 /* offset in string*/);
				xzl_bug_on(text_len != ret);

				record_buffer[k].data.data = p + r;
				record_buffer[k].data.len = text_len;

				/* todo : extract & fill ts?? */

				k++;
				r += text_len;
				show_progress += 1;

				if (k == buffer_size_records) /* enough records. done */
					break;
			}
			if (k != buffer_size_records) {
				EE("warning: %lu vs %lu\n", k, buffer_size_records);
				getchar();
				buffer_size_records = k;
			}
//			xzl_bug_on(k != buffer_size_records);
		}  // numa nodes
	}

	// source, no-op
	virtual ptime RefreshWatermark(ptime wm) override {
		return wm;
	}

	/* internal accounting  -- to be updated by the evaluator*/
	atomic<unsigned long> byte_counter_, record_counter_;

	bool ReportStatistics(PTransform::Statstics* stat) override {

		/* internal accounting */
		unsigned long total_records = record_counter_.load(std::memory_order_relaxed);
		unsigned long total_bytes = byte_counter_.load(std::memory_order_relaxed);

		/* last time we report */
		static unsigned long last_bytes = 0, last_records = 0;
		static ptime last_check, start_time;
		static int once = 1;

		ptime now = boost::posix_time::microsec_clock::local_time();

		if (once) {
			once = 0;
			last_check = now;
			start_time = now;
			last_records = total_records;
			return false;
		}

		boost::posix_time::time_duration diff = now - last_check;

		{
			double interval_sec = (double) diff.total_milliseconds() / 1000;
			double total_sec = (double) (now - start_time).total_milliseconds() / 1000;

			stat->name = this->name.c_str();
			stat->mbps = (double) total_bytes / total_sec;
			stat->mrps = (double) total_records / total_sec;

			stat->lmbps = (double) (total_bytes - last_bytes) / interval_sec;
			stat->lmrps = (double) (total_records - last_records) / interval_sec;

			last_check = now;
			last_bytes = total_bytes;
			last_records = total_records;
		}

		return true;
	}

	void ExecEvaluator(int nodeid, EvaluationBundleContext *c, shared_ptr<BundleBase>) override;

//  void ExecEvaluator(int nodeid, EvaluationBundleContext *c) override {
//  	/* instantiate an evaluator */
//  	UnboundedInMemEvaluator<string_range> eval(nodeid);
//		eval.evaluate(this, c);
//  }

};

#if 0
/* for Join */
//template<>
template<template<class> class BundleT>
class UnboundedInMem<long, BundleT> : public PTransform {
	using T = long;
  using OutputBundleT = RecordBitmapBundle<T>;

public:
  int interval_ms;  /* the time difference between consecutive bundles */
  const int session_gap_ms; /* gap between "bursts" of bundles. for testing session windows */

  UnboundedInMem (string name, int interval_ms = 100, int session_gap_ms = 0)
  : PTransform(name), interval_ms(interval_ms),
    session_gap_ms(session_gap_ms) { }

  /* lockfree
   *
   * @ts: (hacking): the ts for each record in the bundle. to be removed when
   * the source can generate ts.
   * return: # records actually filled in
   * */
  uint64_t FillBundle(int out_id, OutputBundleT & bundle,
  		uint64_t capacity, ptime ts) {
  	uint64_t i;
  	for (i = 0; i < capacity; i++) {
  		bundle.add_record(Record<T>(genRandomElement(out_id), ts));
  	}
  	return i;
  }

  // source, no-op. note that we shouldn't return the transform's wm
  virtual ptime RefreshWatermark(ptime wm) override {
    return wm;
  }


  /* internal accounting  -- to be updated by the evaluator*/
  atomic<unsigned long> byte_counter_, record_counter_;
  bool ReportStatistics(PTransform::Statstics* stat) override {
  	//TODO
	/*
	std::cout << __FILE__ << __LINE__ << "  TODO: ReportStatistics is needed!!!!!!!!!!!!!!" << std::endl;
	//assert(false && "todo...");
	return false;
	*/
    /* internal accounting */
     unsigned long total_records =
    		 record_counter_.load(std::memory_order_relaxed);
     unsigned long total_bytes =
    		 byte_counter_.load(std::memory_order_relaxed);

     /* last time we report */
     static unsigned long last_bytes = 0, last_records = 0;
     static ptime last_check, start_time;
     static int once = 1;

   	ptime now = boost::posix_time::microsec_clock::local_time();

   	if (once) {
   		once = 0;
   		last_check = now;
   		start_time = now;
   		last_records = total_records;
   		return false;
   	}

   	boost::posix_time::time_duration diff = now - last_check;

   	{
   		double interval_sec = (double) diff.total_milliseconds() / 1000;
   		double total_sec = (double) (now - start_time).total_milliseconds() / 1000;

   		stat->name = this->name.c_str();
   		stat->mbps = (double) total_bytes / total_sec;
   		stat->mrps = (double) total_records / total_sec;

   		stat->lmbps = (double) (total_bytes - last_bytes) / interval_sec;
   		stat->lmrps = (double) (total_records - last_records) / interval_sec;

   		last_check = now;
   		last_bytes = total_bytes;
   		last_records = total_records;
   	}

   	return true;
  }
  
  void ExecEvaluator(int nodeid, EvaluationBundleContext *c, shared_ptr<BundleBase>) override;

private:

  T genRandomElement(int output_id) {

  	assert(output_id >= 0 && output_id < 2);

    static atomic<T> cnt0 (0);
    static atomic<T> cnt1 (0);

    if (output_id == 0)
    	return cnt0.fetch_add(1);
    else
    	return cnt1.fetch_add(1);
  }

};

#endif

/* for Join */
//template<>
template<template<class> class BundleT>
class UnboundedInMem<long, BundleT> : public PTransform {
	using T = long;
	using OutputBundleT = RecordBitmapBundle<T>;

public:
	int interval_ms;  /* the time difference between consecutive bundles */
	//const int session_gap_ms; /* gap between "bursts" of bundles. for testing session windows */

	/*new variables*/
	const char * input_fname;
	const int punc_interval_ms = 1000;
	const unsigned long records_per_interval; // # records between two puncs
	int record_len = 0; /* the length covered by each record */
	const int session_gap_ms; // for tesging session windows
	const uint64_t target_tput; // in record/sec
	//uint64_t string_len; /* the length covered by each record */

	//hym: here we don't read ???
	//array of prefilled buffers of record, one for each NUMA node
	//vector<Record<T> *> record_buffers;
	vector<Record<long> *> record_buffers;
	uint64_t buffer_size_records = 0;

	//vector<long const *> buffers; /*buffers, one for each NUMA node*/
	vector<long *> buffers;
	uint64_t buffer_size = 0;
	unsigned long record_num = 0;
#if 0
	UnboundedInMem (string name, int interval_ms = 100, int session_gap_ms = 0)
  : PTransform(name), interval_ms(interval_ms),
    session_gap_ms(session_gap_ms) { }
#endif
	UnboundedInMem (string name, const char *input_fname, unsigned long rpi, uint64_t tt, uint64_t record_size, int session_gap_ms) :
			PTransform(name), input_fname(input_fname), records_per_interval(rpi), record_len(record_size), //= sizeof(long)
			session_gap_ms(session_gap_ms), target_tput(tt), byte_counter_(0), record_counter_(0){
		//TODO
		interval_ms = 50;
		//int fd;
		struct stat finfo;
		//XXX only use one node for test
//	int num_nodes = numa_num_configured_nodes();
		int num_nodes = 1;

		std::cout << "WARNING: num_nodes is set to 1!!!!! Reset it in Unbounded.h and UnboundedInMemEvaluator.h!!!" << std::endl;

		//scan the file to see how many record(intige) the file has
		//int record_num = 0;
		//long i = 0;
		FILE * file = fopen(input_fname, "r");
		if(!file){
			assert(false && "open file failed!!!");
		}

		std::cout << "WARNING: record_num is 282090931. Should set the value according to input file!!!!" << std::endl;
		record_num = 282090931;
//XXX comment this temporarilly!!! Remember to restore this
#if 0
		// get # of records
	// http://stackoverflow.com/questions/4600797/read-int-values-from-a-text-file-in-c
	fscanf(file, "%ld", &i);
	while(!feof(file)){
		record_num ++;
		//std::cout << i << " ";
		fscanf(file, "%ld", &i);
	}
#endif
		// get file size
		// http://stackoverflow.com/questions/238603/how-can-i-get-a-files-size-in-c
		unsigned long fsize;
#if 0   // method 1
		fseek(file, 0, SEEK_END);// seek to end of file
	fsize = ftell(file); // get current file pointer
	fseek(file, 0, SEEK_SET); // seek back to beginning of file
#endif
		// method 2
		stat(input_fname, &finfo);
		fsize = finfo.st_size;
		fsize = fsize; //fix warning

//	buffer_size = record_num * record_len; // record_num * sizeof(long)
		buffer_size = records_per_interval * record_len * 2 * 2;

		/* sanity check: file long enough for the buffer? */
		if ((int64_t)buffer_size > finfo.st_size) {
			EE("input data not enough. need %lu KB. has %lu KB", buffer_size / 1024, finfo.st_size / 1024);
			abort();
		}
		record_num = buffer_size / record_len;

		// print source config info
		printf("---- source configuration ---- \n");
		printf("source file: %s\n", input_fname);
		printf("source file size: %.2f MB\n", (double)finfo.st_size/1024/1024);
		printf("buffer size: %.2f MB\n", (double)buffer_size/1024/1024);
		printf("Number of Records: %ld \n", record_num);

		// allocate and fill buffers: should replace genRandomElement() function
		// XXX shall we allocate a buffer on each node??
		// This may be not a good idea. It's better that two input streams on the same NUMA node
		for(int i = 0; i < num_nodes; i++){
			long *p = (long *)numa_alloc_onnode(buffer_size, i);
			assert(p);
			fseek(file, 0, SEEK_SET); // seek back to beginning of file

			long rcd;
			unsigned long j = 0;
			int ret = fscanf(file, "%ld", &rcd);
			while(!feof(file)){
				if (j == record_num)
					break;
				p[j++] = rcd;
				ret = fscanf(file, "%ld", &rcd);
			}
			ret = ret; //fix warning
			std::cout << "add " << j << " records(long) to a buffer" << std::endl;
			buffers.push_back(p);
		}

		//fill the buffers of records
		for(int i = 0; i < num_nodes; i++){
			Record<T> * record_buffer = (Record<T> *) numa_alloc_onnode(sizeof(Record<T>) * record_num,i);
			assert(record_buffer);
			for(unsigned long j = 0; j < record_num; j++){
				//record_buffer[j].data.data = buffers[i][j];
				record_buffer[j].data = buffers[i][j];
				//std::cout << "record_buffer[j.data] is " << record_buffer[j].data << std::endl;
				//record_buffer[j].data.len = record_len; //sizeof(long)
				//record_buffer[j].ts will be filled by eval
			}
			record_buffers.push_back(record_buffer);
		}

		fclose(file);

	}//end UnboundedInMem init

	/* lockfree
     *
     * @ts: (hacking): the ts for each record in the bundle. to be removed when
     * the source can generate ts.
     * return: # records actually filled in
     * */
	uint64_t FillBundle(int out_id, OutputBundleT & bundle, uint64_t capacity, ptime ts) {
		uint64_t i;
		for (i = 0; i < capacity; i++) {
			bundle.add_record(Record<T>(genRandomElement(out_id), ts));
		}
		return i;
	}

	// source, no-op. note that we shouldn't return the transform's wm
	virtual ptime RefreshWatermark(ptime wm) override {
		return wm;
	}


	/* internal accounting  -- to be updated by the evaluator*/
	atomic<unsigned long> byte_counter_, record_counter_;
	bool ReportStatistics(PTransform::Statstics* stat) override {
		//TODO
		/*
        std::cout << __FILE__ << __LINE__ << "  TODO: ReportStatistics is needed!!!!!!!!!!!!!!" << std::endl;
        //assert(false && "todo...");
        return false;
        */
		/* internal accounting */
		unsigned long total_records = record_counter_.load(std::memory_order_relaxed);
		unsigned long total_bytes = byte_counter_.load(std::memory_order_relaxed);

		/* last time we report */
		static unsigned long last_bytes = 0, last_records = 0;
		static ptime last_check, start_time;
		static int once = 1;

		ptime now = boost::posix_time::microsec_clock::local_time();

		if (once) {
			once = 0;
			last_check = now;
			start_time = now;
			last_records = total_records;
			return false;
		}

		boost::posix_time::time_duration diff = now - last_check;

		{
			double interval_sec = (double) diff.total_milliseconds() / 1000;
			double total_sec = (double) (now - start_time).total_milliseconds() / 1000;

			stat->name = this->name.c_str();
			stat->mbps = (double) total_bytes / total_sec;
			stat->mrps = (double) total_records / total_sec;

			stat->lmbps = (double) (total_bytes - last_bytes) / interval_sec;
			stat->lmrps = (double) (total_records - last_records) / interval_sec;

			last_check = now;
			last_bytes = total_bytes;
			last_records = total_records;
		}

		return true;
	}

	void ExecEvaluator(int nodeid, EvaluationBundleContext *c, shared_ptr<BundleBase>) override;

private:

	T genRandomElement(int output_id) {

		assert(output_id >= 0 && output_id < 2);

		static atomic<T> cnt0 (0);
		static atomic<T> cnt1 (0);

		if (output_id == 0)
			return cnt0.fetch_add(1);
		else
			return cnt1.fetch_add(1);
	}

};


/* for yahoo benchmark */
template<template<class> class BundleT>   /* can't use default argument = RecordBitmapBundle<string_range> */
class UnboundedInMem<Event, BundleT> : public PTransform {

    using T = Event;

public:

    const char * input_fname;

    /* these are event time, irrelevant to the engine processing timing */
    const int punc_interval_ms = 10000;
    const unsigned long records_per_interval;  /* # records between two puncs */
    uint64_t string_len; /* the length covered by each record */
    const int session_gap_ms; /* gap between "bursts" of bundles. for testing session windows */
    const uint64_t target_tput; /* in record/sec */

    /* array of prefilled buffers of records, one for each NUMA node */
//  vector<vector<Record<T>>> records;
    vector<Record<T> *> record_buffers;
    uint64_t buffer_size_records = 0;
    /* # of (duplicated) output streams */
    const int num_outputs;

private:
    vector<Event *> buffers; /* buffers, one for each NUMA node */
    uint64_t buffer_size = 0;

public:
    UnboundedInMem (string name, const char *input_fname, unsigned long rpi, uint64_t tt, uint64_t record_size, int session_gap_ms, text_mode mode = TXT_PLAIN, int num_outputs = 1)
            : PTransform(name), input_fname(input_fname), records_per_interval(rpi), string_len(record_size), session_gap_ms(session_gap_ms), target_tput(tt), num_outputs(num_outputs), byte_counter_(0), record_counter_(0)
    {
        int fd;
        struct stat finfo;
        int num_nodes;

        num_nodes = numa_num_configured_nodes();
        //num_nodes = 1;

        CHECK_ERROR((fd = open(input_fname, O_RDONLY)) < 0);
        CHECK_ERROR(fstat(fd, &finfo) < 0);

        buffer_size = std::max(CONFIG_MIN_PERNODE_BUFFER_SIZE_MB * 1024 *1024,
                               CONFIG_MIN_EPOCHS_PERNODE_BUFFER * records_per_interval * string_len);

        /* sanity check: file long enough for the buffer? */
        if ((int64_t)buffer_size > finfo.st_size) {
            EE("input data not enough. need %.2f MB. has %.2f MB",
               (double) buffer_size / 1024 / 1024, (double) finfo.st_size / 1024 / 1024);
            abort();
        }

        buffer_size_records = buffer_size / string_len;
        xzl_assert(buffer_size_records > 0);

        char *rpath = realpath(input_fname, NULL);
        xzl_bug_on(!rpath);

        printf("---- source configuration ---- \n");
        printf("source file: %s (specified as: %s)\n", rpath, input_fname);
        free(rpath);
        printf("source file size: %.2f MB\n", (double)finfo.st_size/1024/1024);
        printf("buffer size: %.2f MB\n", (double)buffer_size/1024/1024);
        printf("%10s %10s %10s %10s %10s %10s %10s %10s\n",
               "#nodes:", "KRec/nodebuf", "MB", "epoch/ms", "KRec/epoch", "MB/epoc",
               "target:KRec/S", "RecSize" );
        printf("%10d %10lu %10lu %10d %10lu %10lu %10lu %10lu\n",
               num_nodes, buffer_size_records/1000, buffer_size/1024/1024,
               punc_interval_ms, records_per_interval/1000,
               records_per_interval * string_len /1024/1024,
               target_tput/1000, string_len);

        /* get per-node buffers and fill the buffers with the file contents */
        for(int i = 0; i < num_nodes; i++){
            struct Event *p = (struct Event *)numa_alloc_onnode(buffer_size, i);
            assert(p);

            std::ifstream infile(input_fname);//("/home/george/clion-2017.3.3/clionProjects/YahooBenchmark/Data.txt");
            std::string line;
            int j = 0;
            long cnt = 0;
            vector<string> myString;

            boost::progress_display show_progress(buffer_size);


            while (std::getline(infile, line))
            {
                istringstream ss(line);
                string token;
                while (getline(ss, token,',')){
                    //ss.ignore();
                    myString.push_back(token);
                    //cout << myString[i] << endl;
                    j++;
                }

                //time_t time = std::stol(myString[i-7]);
                //time_t now = time(0);
                //auto timestamp = from_time_t(now);
                time_t t = std::time(0);
                long timestamp = static_cast<long int> (t);
                auto user_id = myString[j-6];
                auto page_id = myString[j-5];
                auto ad_id = myString[j-4];
                auto ad_type = myString[j-3];
                //auto event_type = myString[i-2];
                auto num_event_type = (myString[j-2] == "view" ) ? 2 : 0;
                auto ip = std::stoi(myString[j-1]);

                //p[cnt].timeStamp = timestamp;
                p[cnt].user_id = user_id;
                p[cnt].page_id = page_id;
                p[cnt].ad_id = ad_id;
                p[cnt].ad_type = ad_type;
                p[cnt].num_event_type = num_event_type;
                p[cnt].ip = ip;

                cnt ++;


                show_progress += 136;

                if (cnt == buffer_size_records) //131072//163840
                    break;
            }

            buffers.push_back(p);
        }

        //file the buffers of records
        for(int i = 0; i < num_nodes; i++){
            Record<Event> * record_buffer =
                    (Record<Event> *) numa_alloc_onnode(sizeof(Record<Event>) * buffer_size_records, i);
            assert(record_buffer);
            for(long j = 0; j < buffer_size_records; j++){

                record_buffer[j].data = buffers[i][j];//make_pair(convert_ip_pair(buffers[i][j].sd_ip), buffers[i][j].rtt);
            }
            record_buffers.push_back(record_buffer);
        }

//    EE("filled %lu buffers, each with %lu records,"
//    		"each covering size %lu MB from file %s.",
//        buffers.size(), buffer_size_records, buffer_size/1024/1024, input_fname);
    }

    /* ingest tweets json files: extract the ts and text body (as string_range)
     * dispatched from ctor */

    // source, no-op
    virtual ptime RefreshWatermark(ptime wm) override {
        return wm;
    }

    /* internal accounting  -- to be updated by the evaluator*/
    atomic<unsigned long> byte_counter_, record_counter_;

    bool ReportStatistics(PTransform::Statstics* stat) override {

        /* internal accounting */
        unsigned long total_records = record_counter_.load(std::memory_order_relaxed);
        unsigned long total_bytes = byte_counter_.load(std::memory_order_relaxed);

        /* last time we report */
        static unsigned long last_bytes = 0, last_records = 0;
        static ptime last_check, start_time;
        static int once = 1;

        ptime now = boost::posix_time::microsec_clock::local_time();

        if (once) {
            once = 0;
            last_check = now;
            start_time = now;
            last_records = total_records;
            return false;
        }

        boost::posix_time::time_duration diff = now - last_check;

        {
            double interval_sec = (double) diff.total_milliseconds() / 1000;
            double total_sec = (double) (now - start_time).total_milliseconds() / 1000;

            stat->name = this->name.c_str();
            stat->mbps = (double) total_bytes / total_sec;
            stat->mrps = (double) total_records / total_sec;

            stat->lmbps = (double) (total_bytes - last_bytes) / interval_sec;
            stat->lmrps = (double) (total_records - last_records) / interval_sec;

            last_check = now;
            last_bytes = total_bytes;
            last_records = total_records;
        }

        return true;
    }

    void ExecEvaluator(int nodeid, EvaluationBundleContext *c, shared_ptr<BundleBase>) override;

//  void ExecEvaluator(int nodeid, EvaluationBundleContext *c) override {
//  	/* instantiate an evaluator */
//  	UnboundedInMemEvaluator<string_range> eval(nodeid);
//		eval.evaluate(this, c);
//  }

};
///////////////////////////////////////////////////////////////////

template<class T1, class T2,
		class Tuple = tuple<T1,T2>>
class BoundedCol2: public Bounded<Tuple> {
public:
	BoundedCol2(string name, BoundedSource<Tuple>* source) : Bounded<Tuple>(name, source) { }
};

#if 0
template<class T1, class T2,
class Tuple = tuple<T1,T2>>
class PrinterCol2: public PTransform {
  string name;
public:
  PrinterCol2(string name) : PTransform(name) { }

  PCollection<Tuple>* apply(PValue *input) {
    return PCollection<Tuple>::createPrimitiveOutputInternal(
        input->getPipeline(),
        NULL, PCollection<Tuple>::IsBounded::BOUNDED);
  }
};
#endif

template<class T1, class T2,
		class Tuple = tuple<T1,T2>>
class ReadCol2 {
public:
	static BoundedCol2<T1,T2>* from(BoundedSource<Tuple>* source,
									string name = "") {
		return new BoundedCol2<T1,T2>(name, source);
	}
};
#endif /* READ_H_ */

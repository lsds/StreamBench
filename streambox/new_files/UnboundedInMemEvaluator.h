#ifndef UNBOUNDEDINMEMEVALUATOR_H
#define UNBOUNDEDINMEMEVALUATOR_H

extern "C" {
#include "measure.h"
}

//#include "utilities/threading.hh" // for concurrent queue
//#include "ctpl.h" // for watermark workers
#include "Source/Unbounded.h"
#include "UnboundedInMemEvaluatorBase.h"

using namespace Kaskade;

#ifdef MEASURE_LATENCY
extern  boost::posix_time::ptime start; //global variable
#endif

/* no generic impl */
template<class T, template<class> class BundleT> class UnboundedInMemEvaluator;

template<template<class> class BundleT>
class UnboundedInMemEvaluator<string_range, BundleT> : public UnboundedInMemEvaluatorBase<string_range, BundleT> {

    using T = string_range;
    using TransformT = typename UnboundedInMemEvaluatorBase<T, BundleT>::TransformT;
    using BaseT = UnboundedInMemEvaluatorBase<T, BundleT>;

#ifdef USE_NUMA_TP
    using TicketT = Ticket;
#else
    using TicketT = std::future<void>;
#endif

    //  const int record_size = 1024;

    // okay to buffer watermarks and make the wm worker thread to process them
    // in order (we may even batch some wms. But we also have to buffer the corresponding tickets for each
    // watermark.
    //  ConcurrentQueue<ptime> _watermarks;
    //  ctpl::thread_pool _wm_worker;

public:
    /* we are not bound to any node */
    //  UnboundedInMemEvaluator(int node, int record_size = 1024)
    //			: record_size(record_size) { }

    UnboundedInMemEvaluator(int node) { }

    /* Create a task that waits for all outstanding records that arrived before
     * the wm are observed by the downstream) and propagates watermark @ts to
     * downstream.
     *
     *
     * Note that we have to propagate the @ts (as its current snapshot). This is
     * because the source's wm may have been advanced (say to W') by the time the
     * downstream (T) refreshes its wm. Pulling W' from source by then is wrong
     * since we cannot guarantee that all records arrived prior to W' are
     * observed by T.
     *
     * Example: Pipeline         S (source) --> T (downstream)
     *
     * Timeline
     *       r4 [W'] r3 r2 [W] r1
     * --------------------------> stream
     *
     * S's worker thread waits for r1 to reache T before it propagates W to T
     * meanwhile, S continues to produce future records and watermarks.  It may
     * observe the new wm W' and advances its watermark to W' If the worker
     * thread updates T's watermark with W' (instead of W), T may see W' before
     * r2 and r3.  The may violate the watermark semantics.
     *
     * Since wait() may block, this function should not be executed in our thread
     * pool.
     */

    static void SeedSourceWatermarkBlocking(EvaluationBundleContext *c,
                                            shared_ptr<vector<TicketT>> tickets,
                                            const ptime wm)
    {

#if 0 /* we shouldn't block threads from the threadpool */
        NumaThreadPool & pool = NumaThreadPool::instance();

		/* XXX should specify node to run */
		pool.runOnNode(0, Task([=]{
					for (auto && t : *tickets)
					t.wait();
					c->SeedWatermark(ts);
					}));
#endif

#if 0 /* not working since std::async is basically sync */
        std::async (std::launch::async, [=]{
				for (auto && t : *tickets)
				t.wait();
				c->SeedWatermark(ts); /* XXX we'd better create async tasks for watermark */
				});
#endif

        for (auto && t : *tickets)
            t.wait();

        /* propagate source's watermark snapshot downstream. */
        c->PropagateSourceWatermarkSnapshot(wm);
    }

    /* tp version; executed in a separate thread; expecting a thread id */
    static void SeedSourceWatermarkBlockingTP(int id, EvaluationBundleContext *c,
                                              shared_ptr<vector<TicketT>> tickets,
                                              const ptime ts)
    {
        //  	EE("watermark: %s...", to_simple_string(ts).c_str());

        //  	EE("%s: wm: %s...",
        //  			to_simple_string(boost::posix_time::microsec_clock::local_time()).c_str(),
        //  			to_simple_string(ts).c_str());

        SeedSourceWatermarkBlocking(c, tickets, ts);
    }

    /*
     * issue a wave of multi bundles, advance the watermark, then issue again.
     * (current implementation: one wave carries whole content of one buffer)
     *
     * records of the same bundle have the same values of timestamps
     * the timestamp increases between consecutive bundles.
     *
     * NB: this function should not be executed by threadpool worker as it will
     * block.
     */
#if 0
    void evaluate(TransformT* t, EvaluationBundleContext *c,
			shared_ptr<BundleBase> bundle_ptr = nullptr) override {

		boost::posix_time::time_duration delta = milliseconds(t->interval_ms);

#if 1
		/* support at most 2 output streams */
		auto out = t->getFirstOutput();
		//    auto out2 = t->getFirstOutput();
		assert(out);

#ifdef USE_NUMA_TP
		NumaThreadPool& pool = NumaThreadPool::instance();
#endif

		/* # of bundles between two puncs */
		const int bundle_count = numa_num_configured_cpus(); /* # cores */
		//    const int bundle_count = 2;	// debugging

		const uint64_t range_per_bundle = t->buffer_size / bundle_count;
		const int records_per_bundle = range_per_bundle / record_size + 1;

		EE(" ---- punc internal is %lu sec (ev time) --- ",
				delta.total_milliseconds() * bundle_count / 1000 );

		bool is_active = false;

		/* an infi loop that emit bundles to all NUMA nodes periodically.
		 * spawn downstream eval to consume the bundles.
		 * NB: we can only sleep in the main thread (which is not
		 * managed by the NUMA thread pool).
		 */

		while (true) {
#if 1
			BaseT::pause_between_waves();

			if (BaseT::is_pressure_high(c)) {
				static boost::posix_time::ptime last_tick;
				static int count = 0, interval_sec = 3;
				boost::posix_time::ptime this_tick = \
								     boost::posix_time::second_clock::local_time();
				boost::posix_time::time_duration diff = this_tick - last_tick;

				if (diff.total_seconds() > interval_sec) {
					I("downstream pressure too high. backoff (%d msgs in %d sec)",
							count, interval_sec);
					last_tick = this_tick;
					count = 0;
					//      		dump_all_containers(c); // debugging
				} else
					count ++;

				if (is_active)
					EE("XXXXX source stops ");
				is_active = false;
				continue;
			} else {
				if (!is_active)
					EE("XXXXX source starts ");
				is_active = true;
			}
#endif

			/*
			 * each bundle has a fixed number of records of string_range (the range
			 * is also fixed); each bundle is to be consumed by a new task.
			 *
			 * create bundles_per_node * #nodes tasks in parallel.
			 *
			 * will NOT block
			 */

			uint64_t offset = 0;
			const int num_nodes = numa_max_node() + 1;

			for (int i = 0; i < bundle_count; i++) {
				/* send bundles to numa nodes in a round-robin fashion */
				int nodeid = (i % num_nodes);
				/* create a multi-record bundle */

				if (offset >= t->buffer_size)
					offset = 0;

				const char * bundle_start = \
							    t->buffers[nodeid] + range_per_bundle * i;

				shared_ptr<BundleT<T>>
					bundle(make_shared<BundleT<T>>(
								records_per_bundle,  /* reserved capacity */
								nodeid));

#ifdef MEASURE_LATENCY
				bundle->mark("creation at source");
#endif
				assert(bundle);

				string_range range;
				range.len = record_size;

				uint64_t offset = 0;
				/* limitation: XXX
				 * 1. we may miss the last @record_size length in the bundle range
				 * 2. we don't split records at the word boundary.
				 * 3. we don't force each string_range's later char to be \0
				 */

				VV("pack records in bundle ts %s:",
						to_simple_string(current_ts + delta * i).c_str());

				while (offset + record_size < range_per_bundle) {
					range.data = bundle_start + offset;

#if 0
					char buf[20];
					//					  	snprintf(buf, 10, "%s", range.data); // very slow
					memcpy(buf, range.data, 15);
					buf[15] = '\0';
					W("string_range XXX %lx (%s...", (unsigned long)range.data, buf);
#endif
					/* same ts for all records in the bundle */
					bundle->add_record(Record<T>(range, BaseT::current_ts + delta * i));
					offset += record_size;
				}

				out->consumer->depositOneBundle(bundle, nodeid);

				c->SpawnConsumer();
#if 0
				/*----------for measuring latency----------*/
				if(first){
					start = boost::posix_time::second_clock::local_time();
					std::cout << "start time: " << boost::posix_time::to_simple_string(start) << std::endl;
					first = 0;
				}
				/*-----------------------------------------*/
#endif
			} // done sending @bundle_count bundles.

			BaseT::current_ts += delta * bundle_count;
			BaseT::current_ts += milliseconds(t->session_gap_ms);

			/* Make sure all data have left the source before
			   advancing the watermark. otherwise, downstream transforms may see
			   watermark advance before they see the (old) records.

			   While we hold the watermark, however, we don't have to hold records
			   that arrive after the watermark. */

			/*
			 * update source watermark immediately, but propagate the watermark
			 * to downstream asynchronously.
			 */
			c->UpdateSourceWatermark(BaseT::current_ts);

			/* Useful before the sink sees the 1st watermark */
			if (c->GetTargetWm() == max_date_time) { /* unassigned */
				c->SetTargetWm(BaseT::current_ts);
			}

			/* where we process wm.
			 * we spread wm among numa nodes
			 * NB this->_node may be -1 */
			static int wm_node = 0;
			out->consumer->depositOnePunc(make_shared<Punc>(BaseT::current_ts, wm_node),
					wm_node);
			c->SpawnConsumer();
			if (++wm_node == numa_max_node())
				wm_node = 0;

			t->byte_counter_.fetch_add(bundle_count * records_per_bundle * record_size,
					std::memory_order_relaxed);
			t->record_counter_.fetch_add(bundle_count * records_per_bundle,
					std::memory_order_relaxed);

#if 0
			auto b = report_progress(bundle_count * records_per_bundle * record_size,
					bundle_count * records_per_bundle);

			/* also report about thread pool */
			if (b) {
				long total_pending = 0;
				for (auto && v : c->_transforms) {
					/* just peek. we don't grab lock */
					long pending = v->getNumBundles();
					total_pending += pending;
				}

				EE("pending bundles %ld, "
						"processed num_bundles_beforewm %ld num_bundles_afterwm %ld",
						total_pending,
						c->num_bundles_beforewm.load(std::memory_order_relaxed),
						c->num_bundles_afterwm.load(std::memory_order_relaxed));

				/* debugging */
				//      	 dump_all_containers(c);
			}
#endif

		}   // while
#endif
	}
#endif


private:
    /* moved from WindowKeyedReducerEval (XXX merge later)
     * task_id: zero based.
     * return: <start, cnt> */
    static pair<int,int> get_range(int num_items, int num_tasks, int task_id) {
        /* not impl yet */
        xzl_bug_on(num_items == 0);

        xzl_bug_on(task_id > num_tasks - 1);

        int items_per_task  = num_items / num_tasks;

        /* give first @num_items each 1 item */
        if (num_items < num_tasks) {
            if (task_id <= num_items - 1)
                return make_pair(task_id, 1);
            else
                return make_pair(0, 0);
        }

        /* task 0..n-2 has items_per_task items. */
        if (task_id != num_tasks - 1)
            return make_pair(task_id * items_per_task, items_per_task);

        if (task_id == num_tasks - 1) {
            int nitems = num_items - task_id * items_per_task;
            xzl_bug_on(nitems < 0);
            return make_pair(task_id * items_per_task, nitems);
        }

        xzl_bug("bug. never reach here");
        return make_pair(-1, -1);
    }

public:

#include "UnboundedInMemEvaluator_2out.h"

    void evaluate(TransformT* t, EvaluationBundleContext *c,
                  shared_ptr<BundleBase> bundle_ptr = nullptr) override
    {
        if (t->num_outputs == 2) {
            evaluate_2outputs(t, c, bundle_ptr);
            return;
        }

        auto out = t->getFirstOutput();
        assert(out);

        /* # of bundles between two puncs */
        const uint64_t bundle_per_interval
                //= 1 * numa_num_configured_cpus(); /* # cores */
                = 1 * c->num_workers; /* # cores */

        boost::posix_time::time_duration punc_interval =
                milliseconds(t->punc_interval_ms);

        boost::posix_time::time_duration delta =
                milliseconds(t->punc_interval_ms) / bundle_per_interval;

        //    const int bundle_count = 2;	// debugging
        const uint64_t records_per_bundle
                = t->records_per_interval / bundle_per_interval;

        EE(" ---- punc internal is %d sec (ev time) --- ",
           t->punc_interval_ms / 1000);



        cout << "Bundle per interval = " << bundle_per_interval  << "\n";
        cout << "Punc interval = " << to_simple_string(punc_interval).c_str()  << "\n";
        cout << "Delta = " << delta.total_milliseconds()  << "\n";
        cout << "Records per bundle = " << records_per_bundle  << "\n";



        //			bool is_active = false;

        /* an infi loop that emit bundles to all NUMA nodes periodically.
         * spawn downstream eval to consume the bundles.
         * NB: we can only sleep in the main thread (which is not
         * managed by the NUMA thread pool).
         */

        const int num_nodes = numa_max_node() + 1;
        //           uint64_t offsets[8] = {0}; /* in each NUMA record buffer */

        /* the global offset into each NUMA buffer to avoid repeated content.
         * (since each buffer has same content)
         */
        uint64_t offset = 0;
        uint64_t us_per_iteration = 1e6 * t->records_per_interval / t->target_tput; /* the target us */

        cout << "us per iteration: " << us_per_iteration << "\n";
        //		 EE("XXX us_per_iteration %lu", us_per_iteration);

        /* source is also MT. 3 seems good for win-grep */
        const int total_tasks = CONFIG_SOURCE_THREADS;

        vector <std::future<void>> futures;


        printf("Starting adding timestamps to records... \n");


        /*cpu_set_t cpuset;
        CPU_ZERO(&cpuset);
        CPU_SET(0, &cpuset);
        pthread_t current_thread = pthread_self();
        pthread_setaffinity_np(current_thread, sizeof(cpu_set_t), &cpuset);*/

        while (true) {
#if 0
            BaseT::pause_between_waves();

			if (BaseT::is_pressure_high(c)) {
				static boost::posix_time::ptime last_tick;
				static int count = 0, interval_sec = 3;
				boost::posix_time::ptime this_tick = \
								     boost::posix_time::second_clock::local_time();
				boost::posix_time::time_duration diff = this_tick - last_tick;

				if (diff.total_seconds() > interval_sec) {
					I("downstream pressure too high. backoff (%d msgs in %d sec)",
							count, interval_sec);
					last_tick = this_tick;
					count = 0;
					//      		dump_all_containers(c); // debugging
				} else
					count ++;

				if (is_active)
					EE("XXXXX source stops ");
				is_active = false;
				continue;
			} else {
				if (!is_active)
					EE("XXXXX source starts ");
				is_active = true;
			}
#endif

            boost::posix_time::ptime start_tick = boost::posix_time::microsec_clock::local_time();

            /*
             * each bundle is to be consumed by a new task.
             *
             * will NOT block
             */

            printf("This is the start_tick: %s outside the for loops. \n", to_simple_string(start_tick).c_str());

            for (int task_id = 0; task_id < total_tasks; task_id++) {

                /* each worker works on a range of bundles in the epoch */
                //			auto source_task_lambda = [t, &total_tasks, &bundle_per_internval, task_id](int id)
                auto source_task_lambda = [t, &total_tasks, &bundle_per_interval,
                        this, &delta, &out, &c, &records_per_bundle, &num_nodes,
                        task_id, offset](int id)
                {
                    auto range = get_range(bundle_per_interval, total_tasks, task_id);

                    auto local_offset = (offset + records_per_bundle * range.first) % t->buffer_size_records;

                    I("source worker %d: bundle range [%d, %d). record offset %lu; total records %lu",
                      task_id, range.first, range.first + range.second, local_offset,
                      t->buffer_size_records);

                    for (int i = range.first; i < range.first + range.second; i++) {
                        /* construct the bundles by reading NUMA buffers round-robin */
                        int nodeid = (i % num_nodes);

                        /* assemble a bundle by drawing records from the corresponding
                         * NUMA record buffer. */

                        shared_ptr<BundleT<T>>
                                bundle(make_shared<BundleT<T>>(
                                records_per_bundle,  /* reserved capacity */
                                nodeid));

                        xzl_assert(bundle);

                        /* limitation: XXX
                         * 1. we may miss the last @record_size length in the bundle range
                         * 2. we don't split records at the word boundary.
                         * 3. we don't force each string_range's later char to be \0
                         */

                        VV("pack records in bundle ts %s:",
                           to_simple_string(current_ts + delta * i).c_str());

                        for (unsigned int j = 0; j < records_per_bundle; j++, local_offset++) {
                            if (local_offset == t->buffer_size_records)
                                local_offset = 0; /* wrap around */
                            /* rewrite the record ts */
                            t->record_buffers[nodeid][local_offset].ts
                                    = BaseT::current_ts + delta * i;
                            /* same ts for all records in the bundle */
                            bundle->add_record(t->record_buffers[nodeid][local_offset]);
                        }

                        cout << "Timestamp for this run  is: " << to_simple_string(BaseT::current_ts + delta * i).c_str() << "\n";
                        cout << "Bundles " << bundle.use_count() << "\n";
                        out->consumer->depositOneBundle(bundle, nodeid);
                        c->SpawnConsumer();
                    } // done sending @bundle_count bundles.
                };

                /* exec the N-1 task inline */
                if (task_id == total_tasks - 1) {
                    source_task_lambda(0);
                    continue;
                }

                futures.push_back( // store a future
                        c->executor_.push(source_task_lambda) /* submit to task queue */
                );
            }  // end of tasks

            for (auto && f : futures) {
                f.get();
            }
            futures.clear();

            /* advance the global record offset */
            offset += records_per_bundle * bundle_per_interval;
            offset %= t->buffer_size_records;

#if 0 /* single thread */
            for (unsigned int i = 0; i < bundle_per_interval; i++) {
				/* construct the bundles by reading NUMA buffers round-robin */
				int nodeid = (i % num_nodes);
				//          	 auto & offset = offsets[nodeid];

				/* assemble a bundle by drawing records from the corresponding
				 * NUMA record buffer. */

				shared_ptr<BundleT<T>>
					bundle(make_shared<BundleT<T>>(
								records_per_bundle,  /* reserved capacity */
								nodeid));

				xzl_assert(bundle);

				/* limitation: XXX
				 * 1. we may miss the last @record_size length in the bundle range
				 * 2. we don't split records at the word boundary.
				 * 3. we don't force each string_range's later char to be \0
				 */

				VV("pack records in bundle ts %s:",
						to_simple_string(current_ts + delta * i).c_str());

				for (unsigned int j = 0; j < records_per_bundle; j++, offset++) {
					if (offset == t->buffer_size_records)
						offset = 0; /* wrap around */
					/* rewrite the record ts */
					t->record_buffers[nodeid][offset].ts
						= BaseT::current_ts + delta * i;
					/* same ts for all records in the bundle */
					bundle->add_record(t->record_buffers[nodeid][offset]);
				}

				out->consumer->depositOneBundle(bundle, nodeid);
				c->SpawnConsumer();
			} // done sending @bundle_count bundles.
#endif


            t->byte_counter_.fetch_add(t->records_per_interval * t->string_len,
                                       std::memory_order_relaxed);
            t->record_counter_.fetch_add(t->records_per_interval,
                                         std::memory_order_relaxed);

            boost::posix_time::ptime end_tick = \
							    boost::posix_time::microsec_clock::local_time();
            auto elapsed_us = (end_tick - start_tick).total_microseconds();


            cout << "End tick is: " << to_simple_string(end_tick).c_str() << "\n";

            cout << "elapsed_us is: " << elapsed_us << "\n";
            cout << "us per iteration: " << us_per_iteration << "\n";



            xzl_assert(elapsed_us > 0);
            if ((unsigned long)elapsed_us > us_per_iteration)
                EE("warning: source runs at full speed.");
            else {
                usleep(us_per_iteration - elapsed_us);
                EE("source pauses for %lu us", us_per_iteration - elapsed_us);
            }


            BaseT::current_ts += punc_interval;
            BaseT::current_ts += milliseconds(t->session_gap_ms);

            cout << "Watermark tick is: " << to_simple_string(BaseT::current_ts).c_str() << "\n";

            /* Make sure all data have left the source before
               advancing the watermark. otherwise, downstream transforms may see
               watermark advance before they see the (old) records. */

            /*
             * update source watermark immediately, but propagate the watermark
             * to downstream asynchronously.
             */
            c->UpdateSourceWatermark(BaseT::current_ts);

            /* Useful before the sink sees the 1st watermark */
            if (c->GetTargetWm() == max_date_time) { /* unassigned */
                c->SetTargetWm(BaseT::current_ts);
            }

            /* where we process wm.
             * we spread wm among numa nodes
             * NB this->_node may be -1 */
            static int wm_node = 0;
            out->consumer->depositOnePunc(make_shared<Punc>(BaseT::current_ts, wm_node),
                                          wm_node);
            c->SpawnConsumer();
            if (++wm_node == numa_max_node())
                wm_node = 0;

        }   // while
    }  // end of eval()
};

/* owned by hym */
template<template<class> class OutputBundleT>
class UnboundedInMemEvaluator<long, OutputBundleT> : public UnboundedInMemEvaluatorBase<long, OutputBundleT> {

    using T = long;
    using TransformT = typename UnboundedInMemEvaluatorBase<T, OutputBundleT>::TransformT;
    //using OutputBundleT = RecordBitmapBundle<T>;
    using BaseT = UnboundedInMemEvaluatorBase<T, OutputBundleT>;

public:
    /* we are not bound to any node */
    UnboundedInMemEvaluator(int node) { }

    void evaluate(TransformT* t, EvaluationBundleContext *c,
                  shared_ptr<BundleBase> bundle_ptr = nullptr) override {

        PValue* out[] = { t->getFirstOutput(), t->getSecondOutput() };
        assert(out[0]);

        int num_outputs = 1;
        if (out[1])
            num_outputs = 2;

        //# of bundles between two puncs
        const uint64_t bundle_per_interval = 1 * numa_num_configured_cpus();

        boost::posix_time::time_duration punc_interval =
                milliseconds(t->punc_interval_ms);

        boost::posix_time::time_duration delta =
                milliseconds(t->punc_interval_ms) / bundle_per_interval;

        const uint64_t records_per_bundle =
                t->records_per_interval / bundle_per_interval;

        EE(" ---- punc internal is %d sec (ev time) --- ",
           t->punc_interval_ms / 1000);

        //XXX WARNING: comment this temporarilly. Remember to restore this later!!!
        //const int num_nodes = numa_max_node() + 1;
        const int num_nodes = 1;

        uint64_t us_per_iteration =
                1e6 * t->records_per_interval * 2 / t->target_tput; /* the target us */
        //uint64_t offset1 = 0;
        //uint64_t offset2 = 0;
        uint64_t offset[10] = {0};//support 10 input streams at most
        while(true){
            boost::posix_time::ptime start_tick =
                    boost::posix_time::microsec_clock::local_time();
            for(unsigned int i = 0; i < bundle_per_interval; i++){
                int nodeid = i % num_nodes;

                for(int oid = 0; oid < num_outputs; oid++){

                    shared_ptr<OutputBundleT<T>>
                            bundle(make_shared<OutputBundleT<T>>(
                            records_per_bundle,  /* reserved capacity */
                            nodeid)); //XXX always on NODE 0
                    assert(bundle);
                    for(unsigned int j = 0; j < records_per_bundle; j++, offset[oid]++){

                        if(offset[oid] == t->record_num){
                            offset[oid] = 0; //wrap around
                        }
                        t->record_buffers[nodeid][offset[oid]].ts = BaseT::current_ts + delta * i;
                        bundle->add_record(t->record_buffers[nodeid][offset[oid]]);
                    }
                    //t->FillBundle(oid, *bundle, records_per_bundle, 						BaseT::current_ts + delta * i);
                    out[oid]->consumer->depositOneBundle(bundle, 0); //XXX only on node 0
                    //std::cout << "deposit one bundle ++++++" << std::endl;
                    c->SpawnConsumer();
                }
            }//end for: bundle_per_interval

            t->byte_counter_.fetch_add(t->records_per_interval * t->record_len * 2,
                                       std::memory_order_relaxed);

            t->record_counter_.fetch_add(t->records_per_interval * 2,
                                         std::memory_order_relaxed);

            boost::posix_time::ptime end_tick =
                    boost::posix_time::microsec_clock::local_time();
            auto elapsed_us = (end_tick - start_tick).total_microseconds();
            assert(elapsed_us > 0);

            if ((unsigned long)elapsed_us > us_per_iteration)
                EE("warning: source runs at full speed.");
            else {
                usleep(us_per_iteration - elapsed_us);
                I("source pauses for %lu us", us_per_iteration - elapsed_us);
            }

            BaseT::current_ts += punc_interval;
            BaseT::current_ts += milliseconds(t->session_gap_ms);

            c->UpdateSourceWatermark(BaseT::current_ts);

            /* Useful before the sink sees the 1st watermark */
            if (c->GetTargetWm() == max_date_time) { /* unassigned */
                c->SetTargetWm(BaseT::current_ts);
            }

            static int wm_node = 0;
            for(int oid = 0; oid < num_outputs; oid++){
                out[oid]->consumer->depositOnePunc(
                        make_shared<Punc>(BaseT::current_ts, wm_node), wm_node);
                //std::cout << "deposit one punc --------" << std::endl;
                c->SpawnConsumer();
                if (++wm_node == numa_max_node()){
                    wm_node = 0;
                }
            }

        }//end while
    }//end evaluate

#if 0
    // Old version: generate random data
	void evaluate(TransformT* t, EvaluationBundleContext *c,
			shared_ptr<BundleBase> bundle_ptr = nullptr) override {

		//std::cout << "UnboundedInMemEvaluator.h line 517:  evaluate " << std::endl;
		boost::posix_time::time_duration delta = milliseconds(t->interval_ms);

		PValue* out[] = { t->getFirstOutput(), t->getSecondOutput() };
		assert(out[0]);

		int num_outputs = 1;
		if (out[1])
			num_outputs = 2;

		const int bundle_count = numa_num_configured_cpus();
		const int bundles_per_node = bundle_count / (numa_max_node() + 1);
		int a = bundles_per_node; a = a; //used to fix warning
		//XXX const int records_per_bundle = 1024;  /* to be changed */ 

		const int records_per_bundle = 5;  /* to be changed */ 

		//		for(int m = 0; m < 3; m++){
		while(true){
			//pause_between_waves();
#if 0
			//XXX is_pressur_high TODO
			for (int i = 0; i < bundles_per_node; i++) {
				//int nodes = numa_max_node() + 1;
				int nodes = 1; //XXX only on node 0 now
				for (int nodeid = 0; nodeid < nodes; nodeid++) {
					//create a multi-record bundle 

					shared_ptr<RecordBitmapBundle<T>>
						bundle(make_shared<OutputBundleT>(
									records_per_bundle,  /* reserved capacity */
									nodeid));

					assert(bundle);

					t->FillBundle(oid, *bundle, records_per_bundle,
							current_ts + delta * i);
					out[i]->consumer->depositOneBundle(bundle, nodeid);
					//XXX c->SpawnConsumer is usless now actually
					c->SpawnConsumer(out[oid], nodeid);
				}
			}

			current_ts = current_ts + delta * bundle_count;
			current_ts = current_ts + milliseconds(t->session_gap_ms);

			c->UpdateSourceWatermark(current_ts);
			if (c->GetTargetWm() == max_date_time) { /* unassigned */
				c->SetTargetWm(current_ts);
			}

			static int wm_node = 0;
			out[i]->consumer->depositOnePunc(make_shared<Punc>(current_ts, wm_node), wm_node);
#endif
			//XXX is_pressur_high TODO

			// 4 bundles + 1 punctuation on both of out[0] and out[1]
			int bundle_per_container = 2;
			for(int i = 0; i < bundle_per_container; i++){
				//num_outputs should be 2
				//std::cout << "num_outputs is " << num_outputs << std::endl;
				for(int oid = 0; oid < num_outputs; oid++){
					//shared_ptr<RecordBitmapBundle<T>>
					shared_ptr<OutputBundleT<T>>
						bundle(make_shared<OutputBundleT<T>>(
									records_per_bundle,  /* reserved capacity */
									0)); //XXX always on NODE 0

					assert(bundle);
					t->FillBundle(oid, *bundle, records_per_bundle, BaseT::current_ts + delta * i);
					out[oid]->consumer->depositOneBundle(bundle, 0); //XXX only on node 0
#ifdef DEBUG
					std::cout << __FILE__ << ": " <<  __LINE__ << "UnboundedInMem deposit a bundle to " << out[oid]->consumer->getName() << std::endl;
#endif
				}
			}

			BaseT::current_ts = BaseT::current_ts + delta * bundle_per_container;
			BaseT::current_ts = BaseT::current_ts + milliseconds(t->session_gap_ms);

			c->UpdateSourceWatermark(BaseT::current_ts);
			if (c->GetTargetWm() == max_date_time) { /* unassigned */
				c->SetTargetWm(BaseT::current_ts);
			}

			static int wm_node = 0;
			for(int oid = 0; oid < num_outputs; oid++){
#ifdef DEBUG
				std::cout << __FILE__ << ": " <<  __LINE__ << "UnboundedInMem deposit a punc to " << out[oid]->consumer->getName() << std::endl;
#endif
				out[oid]->consumer->depositOnePunc(
						make_shared<Punc>(BaseT::current_ts, wm_node), wm_node);
				c->SpawnConsumer();
			}
		}//end while
	}//end evaluate
#endif

#if 0
    public:
	/* we are not bound to any node */
	UnboundedInMemEvaluator(int node) { }

	void evaluate(TransformT* t, EvaluationBundleContext *c,
			shared_ptr<BundleBase> bundle_ptr = nullptr) override {

#ifdef USE_NUMA_TP // todo

		boost::posix_time::time_duration delta = milliseconds(t->interval_ms);

		PValue* out[] = { t->getFirstOutput(), t->getSecondOutput() };
		assert(out[0]);

		int num_outputs = 1;
		if (out[1])
			num_outputs = 2;

		NumaThreadPool& pool = NumaThreadPool::instance();
		/* # of bundles to emit before pause and advance watermark.
		 * if there's multiple outputs, each will get the same number. */
		const int bundle_count = pool.cpus() * 2;
		//    const int bundle_count = 2;	// debugging
		const int bundles_per_node = bundle_count / pool.nodes();
		const int records_per_bundle = 1024;  /* to be changed */

		while (true) {
#if 0
			W(" ----------- to sleep %d ms (XXX shouldn't sleep?) ", t->interval_ms);
			usleep(t->interval_ms * 1000);
#endif
			pause_between_waves();

			if (is_pressure_high(c)) {
				W("downstream pressure too high. backoff");
				continue;
			}

			/*
			 * issue multi bundles at a time. create bundles_per_node * #nodes
			 * tasks in parallel.
			 *
			 * each bundle only has one string, and spawns a new task.
			 */

			//      vector<Ticket> tickets(bundle_count * num_outputs);
			vector<Ticket> tickets;

			for (int i = 0; i < bundles_per_node; i++) {
				for (int nodeid = 0; nodeid < pool.nodes(); nodeid++) {
					for (int oid = 0; oid < num_outputs; oid ++) {

						/* -- each bundle has multiple fixed-size records -- */
						//            tickets[nodeid * bundles_per_node + i] =
						tickets.push_back(
								pool.runOnNode(nodeid, Task([=] { // lambda.
										/* create a multi-record bundle */
										shared_ptr<RecordBitmapBundle<T>>
										bundle(make_shared<OutputBundleT>(
													records_per_bundle,  /* reserved capacity */
													nodeid));

										assert(bundle);

										t->FillBundle(oid, *bundle, records_per_bundle,
												current_ts + delta * i);

										out[oid]->depositOneBundle(bundle, nodeid);
										c->SpawnConsumer(out[oid], nodeid);

										return 1;
										}))); // end of lambda
					} // for
				} // for
			} // for

			for (auto && t : tickets)
				t.wait();

			// expensive?
			current_ts = current_ts + delta * bundle_count;
			current_ts = current_ts + milliseconds(t->session_gap_ms);
			c->SeedWatermark(current_ts);
		} // while (true)
#endif
	}//end evaluate
#endif
};

/* owned by george */
template<template<class> class BundleT>
//class UnboundedInMemEvaluator<Event, OutputBundleT> : public UnboundedInMemEvaluatorBase<Event, OutputBundleT> {
//
//    using T = Event;
//    using TransformT = typename UnboundedInMemEvaluatorBase<T, OutputBundleT>::TransformT;
//    //using OutputBundleT = RecordBitmapBundle<T>;
//    using BaseT = UnboundedInMemEvaluatorBase<T, OutputBundleT>;
//
//public:
//    /* we are not bound to any node */
//    UnboundedInMemEvaluator(int node) { }
//
//    void evaluate(TransformT* t, EvaluationBundleContext *c, shared_ptr<BundleBase> bundle_ptr = nullptr) override {
//
//        PValue* out[] = { t->getFirstOutput(), t->getSecondOutput() };
//        assert(out[0]);
//
//        int num_outputs = 1;
//        if (out[1])
//            num_outputs = 2;
//
//        //# of bundles between two puncs
//        const uint64_t bundle_per_interval = 1 * numa_num_configured_cpus();
//
//        boost::posix_time::time_duration punc_interval =
//                milliseconds(t->punc_interval_ms);
//
//        boost::posix_time::time_duration delta =
//                milliseconds(t->punc_interval_ms) / bundle_per_interval;
//
//        const uint64_t records_per_bundle =
//                t->records_per_interval / bundle_per_interval;
//
//        EE(" ---- punc internal is %d sec (ev time) --- ",
//           t->punc_interval_ms / 1000);
//
//        //XXX WARNING: comment this temporarilly. Remember to restore this later!!!
//        //const int num_nodes = numa_max_node() + 1;
//        const int num_nodes = 1;
//
//        uint64_t us_per_iteration = 1e6 * t->records_per_interval * 2 / t->target_tput; /* the target us */
//        //uint64_t offset1 = 0;
//        //uint64_t offset2 = 0;
//        uint64_t offset[10] = {0};//support 10 input streams at most
//        while(true){
//            boost::posix_time::ptime start_tick =
//                    boost::posix_time::microsec_clock::local_time();
//            for(unsigned int i = 0; i < bundle_per_interval; i++){
//                int nodeid = i % num_nodes;
//
//                for(int oid = 0; oid < num_outputs; oid++){
//
//                    shared_ptr<OutputBundleT<T>>
//                            bundle(make_shared<OutputBundleT<T>>(
//                            records_per_bundle,  /* reserved capacity */
//                            nodeid)); //XXX always on NODE 0
//                    assert(bundle);
//                    for(unsigned int j = 0; j < records_per_bundle; j++, offset[oid]++){
//
//                        if(offset[oid] == t->record_num){
//                            offset[oid] = 0; //wrap around
//                        }
//                        t->record_buffers[nodeid][offset[oid]].ts = BaseT::current_ts + delta * i;
//                        bundle->add_record(t->record_buffers[nodeid][offset[oid]]);
//                    }
//                    //t->FillBundle(oid, *bundle, records_per_bundle, 						BaseT::current_ts + delta * i);
//                    out[oid]->consumer->depositOneBundle(bundle, 0); //XXX only on node 0
//                    //std::cout << "deposit one bundle ++++++" << std::endl;
//                    c->SpawnConsumer();
//                }
//            }//end for: bundle_per_interval
//
//            t->byte_counter_.fetch_add(t->records_per_interval * t->record_len * 2, std::memory_order_relaxed);
//
//            t->record_counter_.fetch_add(t->records_per_interval * 2,
//                                         std::memory_order_relaxed);
//
//            boost::posix_time::ptime end_tick =
//                    boost::posix_time::microsec_clock::local_time();
//            auto elapsed_us = (end_tick - start_tick).total_microseconds();
//            assert(elapsed_us > 0);
//
//            if ((unsigned long)elapsed_us > us_per_iteration)
//                EE("warning: source runs at full speed.");
//            else {
//                usleep(us_per_iteration - elapsed_us);
//                I("source pauses for %lu us", us_per_iteration - elapsed_us);
//            }
//
//            BaseT::current_ts += punc_interval;
//            BaseT::current_ts += milliseconds(t->session_gap_ms);
//
//            c->UpdateSourceWatermark(BaseT::current_ts);
//
//            /* Useful before the sink sees the 1st watermark */
//            if (c->GetTargetWm() == max_date_time) { /* unassigned */
//                c->SetTargetWm(BaseT::current_ts);
//            }
//
//            static int wm_node = 0;
//            for(int oid = 0; oid < num_outputs; oid++){
//                out[oid]->consumer->depositOnePunc(
//                        make_shared<Punc>(BaseT::current_ts, wm_node), wm_node);
//                //std::cout << "deposit one punc --------" << std::endl;
//                c->SpawnConsumer();
//                if (++wm_node == numa_max_node()){
//                    wm_node = 0;
//                }
//            }
//
//        }//end while
//    }//end evaluate
//
//#if 0
//    // Old version: generate random data
//	void evaluate(TransformT* t, EvaluationBundleContext *c,
//			shared_ptr<BundleBase> bundle_ptr = nullptr) override {
//
//		//std::cout << "UnboundedInMemEvaluator.h line 517:  evaluate " << std::endl;
//		boost::posix_time::time_duration delta = milliseconds(t->interval_ms);
//
//		PValue* out[] = { t->getFirstOutput(), t->getSecondOutput() };
//		assert(out[0]);
//
//		int num_outputs = 1;
//		if (out[1])
//			num_outputs = 2;
//
//		const int bundle_count = numa_num_configured_cpus();
//		const int bundles_per_node = bundle_count / (numa_max_node() + 1);
//		int a = bundles_per_node; a = a; //used to fix warning
//		//XXX const int records_per_bundle = 1024;  /* to be changed */
//
//		const int records_per_bundle = 5;  /* to be changed */
//
//		//		for(int m = 0; m < 3; m++){
//		while(true){
//			//pause_between_waves();
//#if 0
//			//XXX is_pressur_high TODO
//			for (int i = 0; i < bundles_per_node; i++) {
//				//int nodes = numa_max_node() + 1;
//				int nodes = 1; //XXX only on node 0 now
//				for (int nodeid = 0; nodeid < nodes; nodeid++) {
//					//create a multi-record bundle
//
//					shared_ptr<RecordBitmapBundle<T>>
//						bundle(make_shared<OutputBundleT>(
//									records_per_bundle,  /* reserved capacity */
//									nodeid));
//
//					assert(bundle);
//
//					t->FillBundle(oid, *bundle, records_per_bundle,
//							current_ts + delta * i);
//					out[i]->consumer->depositOneBundle(bundle, nodeid);
//					//XXX c->SpawnConsumer is usless now actually
//					c->SpawnConsumer(out[oid], nodeid);
//				}
//			}
//
//			current_ts = current_ts + delta * bundle_count;
//			current_ts = current_ts + milliseconds(t->session_gap_ms);
//
//			c->UpdateSourceWatermark(current_ts);
//			if (c->GetTargetWm() == max_date_time) { /* unassigned */
//				c->SetTargetWm(current_ts);
//			}
//
//			static int wm_node = 0;
//			out[i]->consumer->depositOnePunc(make_shared<Punc>(current_ts, wm_node), wm_node);
//#endif
//			//XXX is_pressur_high TODO
//
//			// 4 bundles + 1 punctuation on both of out[0] and out[1]
//			int bundle_per_container = 2;
//			for(int i = 0; i < bundle_per_container; i++){
//				//num_outputs should be 2
//				//std::cout << "num_outputs is " << num_outputs << std::endl;
//				for(int oid = 0; oid < num_outputs; oid++){
//					//shared_ptr<RecordBitmapBundle<T>>
//					shared_ptr<OutputBundleT<T>>
//						bundle(make_shared<OutputBundleT<T>>(
//									records_per_bundle,  /* reserved capacity */
//									0)); //XXX always on NODE 0
//
//					assert(bundle);
//					t->FillBundle(oid, *bundle, records_per_bundle, BaseT::current_ts + delta * i);
//					out[oid]->consumer->depositOneBundle(bundle, 0); //XXX only on node 0
//#ifdef DEBUG
//					std::cout << __FILE__ << ": " <<  __LINE__ << "UnboundedInMem deposit a bundle to " << out[oid]->consumer->getName() << std::endl;
//#endif
//				}
//			}
//
//			BaseT::current_ts = BaseT::current_ts + delta * bundle_per_container;
//			BaseT::current_ts = BaseT::current_ts + milliseconds(t->session_gap_ms);
//
//			c->UpdateSourceWatermark(BaseT::current_ts);
//			if (c->GetTargetWm() == max_date_time) { /* unassigned */
//				c->SetTargetWm(BaseT::current_ts);
//			}
//
//			static int wm_node = 0;
//			for(int oid = 0; oid < num_outputs; oid++){
//#ifdef DEBUG
//				std::cout << __FILE__ << ": " <<  __LINE__ << "UnboundedInMem deposit a punc to " << out[oid]->consumer->getName() << std::endl;
//#endif
//				out[oid]->consumer->depositOnePunc(
//						make_shared<Punc>(BaseT::current_ts, wm_node), wm_node);
//				c->SpawnConsumer();
//			}
//		}//end while
//	}//end evaluate
//#endif
//
//#if 0
//    public:
//	/* we are not bound to any node */
//	UnboundedInMemEvaluator(int node) { }
//
//	void evaluate(TransformT* t, EvaluationBundleContext *c,
//			shared_ptr<BundleBase> bundle_ptr = nullptr) override {
//
//#ifdef USE_NUMA_TP // todo
//
//		boost::posix_time::time_duration delta = milliseconds(t->interval_ms);
//
//		PValue* out[] = { t->getFirstOutput(), t->getSecondOutput() };
//		assert(out[0]);
//
//		int num_outputs = 1;
//		if (out[1])
//			num_outputs = 2;
//
//		NumaThreadPool& pool = NumaThreadPool::instance();
//		/* # of bundles to emit before pause and advance watermark.
//		 * if there's multiple outputs, each will get the same number. */
//		const int bundle_count = pool.cpus() * 2;
//		//    const int bundle_count = 2;	// debugging
//		const int bundles_per_node = bundle_count / pool.nodes();
//		const int records_per_bundle = 1024;  /* to be changed */
//
//		while (true) {
//#if 0
//			W(" ----------- to sleep %d ms (XXX shouldn't sleep?) ", t->interval_ms);
//			usleep(t->interval_ms * 1000);
//#endif
//			pause_between_waves();
//
//			if (is_pressure_high(c)) {
//				W("downstream pressure too high. backoff");
//				continue;
//			}
//
//			/*
//			 * issue multi bundles at a time. create bundles_per_node * #nodes
//			 * tasks in parallel.
//			 *
//			 * each bundle only has one string, and spawns a new task.
//			 */
//
//			//      vector<Ticket> tickets(bundle_count * num_outputs);
//			vector<Ticket> tickets;
//
//			for (int i = 0; i < bundles_per_node; i++) {
//				for (int nodeid = 0; nodeid < pool.nodes(); nodeid++) {
//					for (int oid = 0; oid < num_outputs; oid ++) {
//
//						/* -- each bundle has multiple fixed-size records -- */
//						//            tickets[nodeid * bundles_per_node + i] =
//						tickets.push_back(
//								pool.runOnNode(nodeid, Task([=] { // lambda.
//										/* create a multi-record bundle */
//										shared_ptr<RecordBitmapBundle<T>>
//										bundle(make_shared<OutputBundleT>(
//													records_per_bundle,  /* reserved capacity */
//													nodeid));
//
//										assert(bundle);
//
//										t->FillBundle(oid, *bundle, records_per_bundle,
//												current_ts + delta * i);
//
//										out[oid]->depositOneBundle(bundle, nodeid);
//										c->SpawnConsumer(out[oid], nodeid);
//
//										return 1;
//										}))); // end of lambda
//					} // for
//				} // for
//			} // for
//
//			for (auto && t : tickets)
//				t.wait();
//
//			// expensive?
//			current_ts = current_ts + delta * bundle_count;
//			current_ts = current_ts + milliseconds(t->session_gap_ms);
//			c->SeedWatermark(current_ts);
//		} // while (true)
//#endif
//	}//end evaluate
//#endif
//};
class UnboundedInMemEvaluator<Event, BundleT> : public UnboundedInMemEvaluatorBase<Event, BundleT> {

    using T = Event;
    using TransformT = typename UnboundedInMemEvaluatorBase<T, BundleT>::TransformT;
    using BaseT = UnboundedInMemEvaluatorBase<T, BundleT>;

#ifdef USE_NUMA_TP
    using TicketT = Ticket;
#else
    using TicketT = std::future<void>;
#endif

    //  const int record_size = 1024;

    // okay to buffer watermarks and make the wm worker thread to process them
    // in order (we may even batch some wms. But we also have to buffer the corresponding tickets for each
    // watermark.
    //  ConcurrentQueue<ptime> _watermarks;
    //  ctpl::thread_pool _wm_worker;

public:
    /* we are not bound to any node */
    //  UnboundedInMemEvaluator(int node, int record_size = 1024)
    //			: record_size(record_size) { }

    UnboundedInMemEvaluator(int node) { }

    /* Create a task that waits for all outstanding records that arrived before
     * the wm are observed by the downstream) and propagates watermark @ts to
     * downstream.
     *
     *
     * Note that we have to propagate the @ts (as its current snapshot). This is
     * because the source's wm may have been advanced (say to W') by the time the
     * downstream (T) refreshes its wm. Pulling W' from source by then is wrong
     * since we cannot guarantee that all records arrived prior to W' are
     * observed by T.
     *
     * Example: Pipeline         S (source) --> T (downstream)
     *
     * Timeline
     *       r4 [W'] r3 r2 [W] r1
     * --------------------------> stream
     *
     * S's worker thread waits for r1 to reache T before it propagates W to T
     * meanwhile, S continues to produce future records and watermarks.  It may
     * observe the new wm W' and advances its watermark to W' If the worker
     * thread updates T's watermark with W' (instead of W), T may see W' before
     * r2 and r3.  The may violate the watermark semantics.
     *
     * Since wait() may block, this function should not be executed in our thread
     * pool.
     */

    static void SeedSourceWatermarkBlocking(EvaluationBundleContext *c,
                                            shared_ptr<vector<TicketT>> tickets,
                                            const ptime wm)
    {

#if 0 /* we shouldn't block threads from the threadpool */
        NumaThreadPool & pool = NumaThreadPool::instance();

		/* XXX should specify node to run */
		pool.runOnNode(0, Task([=]{
					for (auto && t : *tickets)
					t.wait();
					c->SeedWatermark(ts);
					}));
#endif

#if 0 /* not working since std::async is basically sync */
        std::async (std::launch::async, [=]{
				for (auto && t : *tickets)
				t.wait();
				c->SeedWatermark(ts); /* XXX we'd better create async tasks for watermark */
				});
#endif

        for (auto && t : *tickets)
            t.wait();

        /* propagate source's watermark snapshot downstream. */
        c->PropagateSourceWatermarkSnapshot(wm);
    }

    /* tp version; executed in a separate thread; expecting a thread id */
    static void SeedSourceWatermarkBlockingTP(int id, EvaluationBundleContext *c,
                                              shared_ptr<vector<TicketT>> tickets,
                                              const ptime ts)
    {
        //  	EE("watermark: %s...", to_simple_string(ts).c_str());

        //  	EE("%s: wm: %s...",
        //  			to_simple_string(boost::posix_time::microsec_clock::local_time()).c_str(),
        //  			to_simple_string(ts).c_str());

        SeedSourceWatermarkBlocking(c, tickets, ts);
    }

    /*
     * issue a wave of multi bundles, advance the watermark, then issue again.
     * (current implementation: one wave carries whole content of one buffer)
     *
     * records of the same bundle have the same values of timestamps
     * the timestamp increases between consecutive bundles.
     *
     * NB: this function should not be executed by threadpool worker as it will
     * block.
     */
#if 0
    void evaluate(TransformT* t, EvaluationBundleContext *c,
			shared_ptr<BundleBase> bundle_ptr = nullptr) override {

		boost::posix_time::time_duration delta = milliseconds(t->interval_ms);

#if 1
		/* support at most 2 output streams */
		auto out = t->getFirstOutput();
		//    auto out2 = t->getFirstOutput();
		assert(out);

#ifdef USE_NUMA_TP
		NumaThreadPool& pool = NumaThreadPool::instance();
#endif

		/* # of bundles between two puncs */
		const int bundle_count = numa_num_configured_cpus(); /* # cores */
		//    const int bundle_count = 2;	// debugging

		const uint64_t range_per_bundle = t->buffer_size / bundle_count;
		const int records_per_bundle = range_per_bundle / record_size + 1;

		EE(" ---- punc internal is %lu sec (ev time) --- ",
				delta.total_milliseconds() * bundle_count / 1000 );

		bool is_active = false;

		/* an infi loop that emit bundles to all NUMA nodes periodically.
		 * spawn downstream eval to consume the bundles.
		 * NB: we can only sleep in the main thread (which is not
		 * managed by the NUMA thread pool).
		 */

		while (true) {
#if 1
			BaseT::pause_between_waves();

			if (BaseT::is_pressure_high(c)) {
				static boost::posix_time::ptime last_tick;
				static int count = 0, interval_sec = 3;
				boost::posix_time::ptime this_tick = \
								     boost::posix_time::second_clock::local_time();
				boost::posix_time::time_duration diff = this_tick - last_tick;

				if (diff.total_seconds() > interval_sec) {
					I("downstream pressure too high. backoff (%d msgs in %d sec)",
							count, interval_sec);
					last_tick = this_tick;
					count = 0;
					//      		dump_all_containers(c); // debugging
				} else
					count ++;

				if (is_active)
					EE("XXXXX source stops ");
				is_active = false;
				continue;
			} else {
				if (!is_active)
					EE("XXXXX source starts ");
				is_active = true;
			}
#endif

			/*
			 * each bundle has a fixed number of records of string_range (the range
			 * is also fixed); each bundle is to be consumed by a new task.
			 *
			 * create bundles_per_node * #nodes tasks in parallel.
			 *
			 * will NOT block
			 */

			uint64_t offset = 0;
			const int num_nodes = numa_max_node() + 1;

			for (int i = 0; i < bundle_count; i++) {
				/* send bundles to numa nodes in a round-robin fashion */
				int nodeid = (i % num_nodes);
				/* create a multi-record bundle */

				if (offset >= t->buffer_size)
					offset = 0;

				const char * bundle_start = \
							    t->buffers[nodeid] + range_per_bundle * i;

				shared_ptr<BundleT<T>>
					bundle(make_shared<BundleT<T>>(
								records_per_bundle,  /* reserved capacity */
								nodeid));

#ifdef MEASURE_LATENCY
				bundle->mark("creation at source");
#endif
				assert(bundle);

				string_range range;
				range.len = record_size;

				uint64_t offset = 0;
				/* limitation: XXX
				 * 1. we may miss the last @record_size length in the bundle range
				 * 2. we don't split records at the word boundary.
				 * 3. we don't force each string_range's later char to be \0
				 */

				VV("pack records in bundle ts %s:",
						to_simple_string(current_ts + delta * i).c_str());

				while (offset + record_size < range_per_bundle) {
					range.data = bundle_start + offset;

#if 0
					char buf[20];
					//					  	snprintf(buf, 10, "%s", range.data); // very slow
					memcpy(buf, range.data, 15);
					buf[15] = '\0';
					W("string_range XXX %lx (%s...", (unsigned long)range.data, buf);
#endif
					/* same ts for all records in the bundle */
					bundle->add_record(Record<T>(range, BaseT::current_ts + delta * i));
					offset += record_size;
				}

				out->consumer->depositOneBundle(bundle, nodeid);

				c->SpawnConsumer();
#if 0
				/*----------for measuring latency----------*/
				if(first){
					start = boost::posix_time::second_clock::local_time();
					std::cout << "start time: " << boost::posix_time::to_simple_string(start) << std::endl;
					first = 0;
				}
				/*-----------------------------------------*/
#endif
			} // done sending @bundle_count bundles.

			BaseT::current_ts += delta * bundle_count;
			BaseT::current_ts += milliseconds(t->session_gap_ms);

			/* Make sure all data have left the source before
			   advancing the watermark. otherwise, downstream transforms may see
			   watermark advance before they see the (old) records.

			   While we hold the watermark, however, we don't have to hold records
			   that arrive after the watermark. */

			/*
			 * update source watermark immediately, but propagate the watermark
			 * to downstream asynchronously.
			 */
			c->UpdateSourceWatermark(BaseT::current_ts);

			/* Useful before the sink sees the 1st watermark */
			if (c->GetTargetWm() == max_date_time) { /* unassigned */
				c->SetTargetWm(BaseT::current_ts);
			}

			/* where we process wm.
			 * we spread wm among numa nodes
			 * NB this->_node may be -1 */
			static int wm_node = 0;
			out->consumer->depositOnePunc(make_shared<Punc>(BaseT::current_ts, wm_node),
					wm_node);
			c->SpawnConsumer();
			if (++wm_node == numa_max_node())
				wm_node = 0;

			t->byte_counter_.fetch_add(bundle_count * records_per_bundle * record_size,
					std::memory_order_relaxed);
			t->record_counter_.fetch_add(bundle_count * records_per_bundle,
					std::memory_order_relaxed);

#if 0
			auto b = report_progress(bundle_count * records_per_bundle * record_size,
					bundle_count * records_per_bundle);

			/* also report about thread pool */
			if (b) {
				long total_pending = 0;
				for (auto && v : c->_transforms) {
					/* just peek. we don't grab lock */
					long pending = v->getNumBundles();
					total_pending += pending;
				}

				EE("pending bundles %ld, "
						"processed num_bundles_beforewm %ld num_bundles_afterwm %ld",
						total_pending,
						c->num_bundles_beforewm.load(std::memory_order_relaxed),
						c->num_bundles_afterwm.load(std::memory_order_relaxed));

				/* debugging */
				//      	 dump_all_containers(c);
			}
#endif

		}   // while
#endif
	}
#endif


private:
    /* moved from WindowKeyedReducerEval (XXX merge later)
     * task_id: zero based.
     * return: <start, cnt> */
    static pair<int,int> get_range(int num_items, int num_tasks, int task_id) {
        /* not impl yet */
        xzl_bug_on(num_items == 0);

        xzl_bug_on(task_id > num_tasks - 1);

        int items_per_task  = num_items / num_tasks;

        /* give first @num_items each 1 item */
        if (num_items < num_tasks) {
            if (task_id <= num_items - 1)
                return make_pair(task_id, 1);
            else
                return make_pair(0, 0);
        }

        /* task 0..n-2 has items_per_task items. */
        if (task_id != num_tasks - 1)
            return make_pair(task_id * items_per_task, items_per_task);

        if (task_id == num_tasks - 1) {
            int nitems = num_items - task_id * items_per_task;
            xzl_bug_on(nitems < 0);
            return make_pair(task_id * items_per_task, nitems);
        }

        xzl_bug("bug. never reach here");
        return make_pair(-1, -1);
    }

public:

#include "UnboundedInMemEvaluator_2out.h"

    void evaluate(TransformT* t, EvaluationBundleContext *c,
                  shared_ptr<BundleBase> bundle_ptr = nullptr) override
    {

        auto out = t->getFirstOutput();
        assert(out);

        /* # of bundles between two puncs */
        const uint64_t bundle_per_interval
                //= 1 * numa_num_configured_cpus(); /* # cores */
                = 1 * c->num_workers; /* # cores */

        boost::posix_time::time_duration punc_interval =
                milliseconds(t->punc_interval_ms);

        boost::posix_time::time_duration delta =
                milliseconds(t->punc_interval_ms) / bundle_per_interval;

        //    const int bundle_count = 2;	// debugging
        const uint64_t records_per_bundle
                = (t->records_per_interval / bundle_per_interval)*CONFIG_SOURCE_THREADS;

        EE(" ---- punc internal is %d sec (ev time) --- ",
           t->punc_interval_ms / 1000);

        //			bool is_active = false;

        /* an infi loop that emit bundles to all NUMA nodes periodically.
         * spawn downstream eval to consume the bundles.
         * NB: we can only sleep in the main thread (which is not
         * managed by the NUMA thread pool).
         */

        const int num_nodes = numa_max_node() + 1;
        //           uint64_t offsets[8] = {0}; /* in each NUMA record buffer */

        /* the global offset into each NUMA buffer to avoid repeated content.
         * (since each buffer has same content)
         */
        uint64_t offset = 0;
        uint64_t us_per_iteration = 1e6 * t->records_per_interval / (t->target_tput/CONFIG_SOURCE_THREADS); /* the target us */

        //		 EE("XXX us_per_iteration %lu", us_per_iteration);

        /* source is also MT. 3 seems good for win-grep */
        const int total_tasks = CONFIG_SOURCE_THREADS;

        const int iter = 1;//t->punc_interval_ms/1000;

        vector <std::future<void>> futures;


        printf("Starting adding timestamps to records... \n");


        while (true) {
#if 0
            BaseT::pause_between_waves();

			if (BaseT::is_pressure_high(c)) {
				static boost::posix_time::ptime last_tick;
				static int count = 0, interval_sec = 3;
				boost::posix_time::ptime this_tick = \
								     boost::posix_time::second_clock::local_time();
				boost::posix_time::time_duration diff = this_tick - last_tick;

				if (diff.total_seconds() > interval_sec) {
					I("downstream pressure too high. backoff (%d msgs in %d sec)",
							count, interval_sec);
					last_tick = this_tick;
					count = 0;
					//      		dump_all_containers(c); // debugging
				} else
					count ++;

				if (is_active)
					EE("XXXXX source stops ");
				is_active = false;
				continue;
			} else {
				if (!is_active)
					EE("XXXXX source starts ");
				is_active = true;
			}
#endif

            /*
             * each bundle is to be consumed by a new task.
             *
             * will NOT block
             */

            for (int it = 0; it < iter; it++) {

                boost::posix_time::ptime start_tick = boost::posix_time::microsec_clock::local_time();
                printf("This is the start_tick: %s outside the for loops. \n", to_simple_string(start_tick).c_str());

                for (int task_id = 0; task_id < total_tasks; task_id++) {

                    /* each worker works on a range of bundles in the epoch */
                    //			auto source_task_lambda = [t, &total_tasks, &bundle_per_internval, task_id](int id)
                    auto source_task_lambda = [t, &total_tasks, &bundle_per_interval,
                            this, &delta, &out, &c, &records_per_bundle, &num_nodes,
                            task_id, offset](int id) {

                        cpu_set_t cpuset;
                        CPU_ZERO(&cpuset);
                        int cpu_core = (2 * task_id + 1) %20;
                        CPU_SET((cpu_core + 1)%20, &cpuset);
                        pthread_t current_thread = pthread_self();
                        std::cout << "Generator Thread #" << task_id << ": on CPU " << sched_getcpu() << "\n";

                        pthread_setaffinity_np(current_thread, sizeof(cpu_set_t), &cpuset);

                        auto range = get_range(bundle_per_interval, total_tasks, task_id);

                        auto local_offset = (offset + records_per_bundle * range.first) % t->buffer_size_records;

                        I("source worker %d: bundle range [%d, %d). record offset %lu; total records %lu",
                          task_id, range.first, range.first + range.second, local_offset,
                          t->buffer_size_records);

                        for (int i = range.first; i < range.first + range.second; i++) {
                            /* construct the bundles by reading NUMA buffers round-robin */
                            int nodeid = (i % num_nodes);

                            /* assemble a bundle by drawing records from the corresponding
                             * NUMA record buffer. */

                            shared_ptr<BundleT<T>>
                                    bundle(make_shared<BundleT<T>>(
                                    records_per_bundle,  /* reserved capacity */
                                    nodeid));

                            xzl_assert(bundle);

                            /* limitation: XXX
                             * 1. we may miss the last @record_size length in the bundle range
                             * 2. we don't split records at the word boundary.
                             * 3. we don't force each string_range's later char to be \0
                             */

                            VV("pack records in bundle ts %s:",
                               to_simple_string(current_ts + delta * i).c_str());

                            //for (unsigned int j = 0; j < records_per_bundle/64; j++, local_offset++) {
                            for (unsigned int j = 0; j < records_per_bundle; j++, local_offset++) {
                                if (local_offset == t->buffer_size_records)
                                    local_offset = 0; /* wrap around */
                                /* rewrite the record ts */
                                t->record_buffers[nodeid][local_offset].ts
                                        = BaseT::current_ts + delta * i;

                                bundle->add_record(t->record_buffers[nodeid][local_offset]);

                                /*bundle->add_record(t->record_buffers[nodeid][local_offset]);
                                bundle->add_record(t->record_buffers[nodeid][local_offset]);
                                bundle->add_record(t->record_buffers[nodeid][local_offset]);
                                bundle->add_record(t->record_buffers[nodeid][local_offset]);
                                bundle->add_record(t->record_buffers[nodeid][local_offset]);
                                bundle->add_record(t->record_buffers[nodeid][local_offset]);
                                bundle->add_record(t->record_buffers[nodeid][local_offset]);

                                bundle->add_record(t->record_buffers[nodeid][local_offset]);
                                bundle->add_record(t->record_buffers[nodeid][local_offset]);
                                bundle->add_record(t->record_buffers[nodeid][local_offset]);
                                bundle->add_record(t->record_buffers[nodeid][local_offset]);
                                bundle->add_record(t->record_buffers[nodeid][local_offset]);
                                bundle->add_record(t->record_buffers[nodeid][local_offset]);
                                bundle->add_record(t->record_buffers[nodeid][local_offset]);
                                bundle->add_record(t->record_buffers[nodeid][local_offset]);

                                local_offset++;
                                if (local_offset == t->buffer_size_records)
                                    local_offset = 0;
                                t->record_buffers[nodeid][local_offset].ts
                                        = BaseT::current_ts + delta * i;
                                bundle->add_record(t->record_buffers[nodeid][local_offset]);
                                bundle->add_record(t->record_buffers[nodeid][local_offset]);
                                bundle->add_record(t->record_buffers[nodeid][local_offset]);
                                bundle->add_record(t->record_buffers[nodeid][local_offset]);
                                bundle->add_record(t->record_buffers[nodeid][local_offset]);
                                bundle->add_record(t->record_buffers[nodeid][local_offset]);
                                bundle->add_record(t->record_buffers[nodeid][local_offset]);
                                bundle->add_record(t->record_buffers[nodeid][local_offset]);

                                bundle->add_record(t->record_buffers[nodeid][local_offset]);
                                bundle->add_record(t->record_buffers[nodeid][local_offset]);
                                bundle->add_record(t->record_buffers[nodeid][local_offset]);
                                bundle->add_record(t->record_buffers[nodeid][local_offset]);
                                bundle->add_record(t->record_buffers[nodeid][local_offset]);
                                bundle->add_record(t->record_buffers[nodeid][local_offset]);
                                bundle->add_record(t->record_buffers[nodeid][local_offset]);
                                bundle->add_record(t->record_buffers[nodeid][local_offset]);

                                local_offset++;
                                if (local_offset == t->buffer_size_records)
                                    local_offset = 0;
                                t->record_buffers[nodeid][local_offset].ts
                                        = BaseT::current_ts + delta * i;
                                bundle->add_record(t->record_buffers[nodeid][local_offset]);
                                bundle->add_record(t->record_buffers[nodeid][local_offset]);
                                bundle->add_record(t->record_buffers[nodeid][local_offset]);
                                bundle->add_record(t->record_buffers[nodeid][local_offset]);
                                bundle->add_record(t->record_buffers[nodeid][local_offset]);
                                bundle->add_record(t->record_buffers[nodeid][local_offset]);
                                bundle->add_record(t->record_buffers[nodeid][local_offset]);
                                bundle->add_record(t->record_buffers[nodeid][local_offset]);

                                bundle->add_record(t->record_buffers[nodeid][local_offset]);
                                bundle->add_record(t->record_buffers[nodeid][local_offset]);
                                bundle->add_record(t->record_buffers[nodeid][local_offset]);
                                bundle->add_record(t->record_buffers[nodeid][local_offset]);
                                bundle->add_record(t->record_buffers[nodeid][local_offset]);
                                bundle->add_record(t->record_buffers[nodeid][local_offset]);
                                bundle->add_record(t->record_buffers[nodeid][local_offset]);
                                bundle->add_record(t->record_buffers[nodeid][local_offset]);

                                local_offset++;
                                if (local_offset == t->buffer_size_records)
                                    local_offset = 0;
                                t->record_buffers[nodeid][local_offset].ts
                                        = BaseT::current_ts + delta * i;
                                bundle->add_record(t->record_buffers[nodeid][local_offset]);
                                bundle->add_record(t->record_buffers[nodeid][local_offset]);
                                bundle->add_record(t->record_buffers[nodeid][local_offset]);
                                bundle->add_record(t->record_buffers[nodeid][local_offset]);
                                bundle->add_record(t->record_buffers[nodeid][local_offset]);
                                bundle->add_record(t->record_buffers[nodeid][local_offset]);
                                bundle->add_record(t->record_buffers[nodeid][local_offset]);
                                bundle->add_record(t->record_buffers[nodeid][local_offset]);

                                bundle->add_record(t->record_buffers[nodeid][local_offset]);
                                bundle->add_record(t->record_buffers[nodeid][local_offset]);
                                bundle->add_record(t->record_buffers[nodeid][local_offset]);
                                bundle->add_record(t->record_buffers[nodeid][local_offset]);
                                bundle->add_record(t->record_buffers[nodeid][local_offset]);
                                bundle->add_record(t->record_buffers[nodeid][local_offset]);
                                bundle->add_record(t->record_buffers[nodeid][local_offset]);
                                bundle->add_record(t->record_buffers[nodeid][local_offset]);*/
                            }

                            out->consumer->depositOneBundle(bundle, nodeid);
                            c->SpawnConsumer();
                        } // done sending @bundle_count bundles.
                    };

                    /* exec the N-1 task inline */
                    if (task_id == total_tasks - 1) {
                        source_task_lambda(0);
                        continue;
                    }

                    futures.push_back( // store a future
                            c->executor_.push(source_task_lambda) /* submit to task queue */
                    );
                }  // end of tasks

                for (auto &&f : futures) {
                    f.get();
                }
                futures.clear();

                /* advance the global record offset */
                offset += records_per_bundle * bundle_per_interval;
                offset %= t->buffer_size_records;

#if 0 /* single thread */
                for (unsigned int i = 0; i < bundle_per_interval; i++) {
                    /* construct the bundles by reading NUMA buffers round-robin */
                    int nodeid = (i % num_nodes);
                    //          	 auto & offset = offsets[nodeid];

                    /* assemble a bundle by drawing records from the corresponding
                     * NUMA record buffer. */

                    shared_ptr<BundleT<T>>
                        bundle(make_shared<BundleT<T>>(
                                    records_per_bundle,  /* reserved capacity */
                                    nodeid));

                    xzl_assert(bundle);

                    /* limitation: XXX
                     * 1. we may miss the last @record_size length in the bundle range
                     * 2. we don't split records at the word boundary.
                     * 3. we don't force each string_range's later char to be \0
                     */

                    VV("pack records in bundle ts %s:",
                            to_simple_string(current_ts + delta * i).c_str());

                    for (unsigned int j = 0; j < records_per_bundle; j++, offset++) {
                        if (offset == t->buffer_size_records)
                            offset = 0; /* wrap around */
                        /* rewrite the record ts */
                        t->record_buffers[nodeid][offset].ts
                            = BaseT::current_ts + delta * i;
                        /* same ts for all records in the bundle */
                        bundle->add_record(t->record_buffers[nodeid][offset]);
                    }

                    out->consumer->depositOneBundle(bundle, nodeid);
                    c->SpawnConsumer();
                } // done sending @bundle_count bundles.
#endif


                t->byte_counter_.fetch_add(t->records_per_interval * t->string_len,
                                           std::memory_order_relaxed);
                t->record_counter_.fetch_add(t->records_per_interval,
                                             std::memory_order_relaxed);

                boost::posix_time::ptime end_tick = boost::posix_time::microsec_clock::local_time();
                auto elapsed_us = (end_tick - start_tick).total_microseconds();


                cout << "End tick is: " << to_simple_string(end_tick).c_str() << "\n";

                cout << "elapsed_us is: " << elapsed_us << "\n";
                cout << "us per iteration: " << us_per_iteration << "\n";

                xzl_assert(elapsed_us > 0);
                if ((unsigned long) elapsed_us > us_per_iteration)
                    EE("warning: source runs at full speed.");
                else {
                    usleep(us_per_iteration - elapsed_us);
                    EE("source pauses for %lu us", us_per_iteration - elapsed_us);
                }

            }
            BaseT::current_ts += punc_interval;
            BaseT::current_ts += milliseconds(t->session_gap_ms);

            /* Make sure all data have left the source before
               advancing the watermark. otherwise, downstream transforms may see
               watermark advance before they see the (old) records. */

            /*
             * update source watermark immediately, but propagate the watermark
             * to downstream asynchronously.
             */
            c->UpdateSourceWatermark(BaseT::current_ts);

            /* Useful before the sink sees the 1st watermark */
            if (c->GetTargetWm() == max_date_time) { /* unassigned */
                c->SetTargetWm(BaseT::current_ts);
            }

            /* where we process wm.
             * we spread wm among numa nodes
             * NB this->_node may be -1 */
            static int wm_node = 0;
            out->consumer->depositOnePunc(make_shared<Punc>(BaseT::current_ts, wm_node),
                                          wm_node);
            c->SpawnConsumer();
            if (++wm_node == numa_max_node())
                wm_node = 0;

        }   // while
    }  // end of eval()
};

#endif /* UNBOUNDEDINMEMEVALUATOR_H */

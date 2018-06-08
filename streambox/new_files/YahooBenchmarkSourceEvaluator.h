#ifndef YAHOOBENCHMARKEVALUATOR_H
#define YAHOOBENCHMARKEVALUATOR_H

extern "C" {
#include "measure.h"
}

#include "core/TransformEvaluator.h"
#include "core/EvaluationBundleContext.h"
#include "YahooBenchmarkSource.h"
#include "UnboundedInMemEvaluatorBase.h"

class YahooBenchmarkSourceEvaluator
        : public TransformEvaulator<YahooBenchmarkSource>{
    using T = struct Event;
    using OutputBundleT = RecordBundle<Event>;
    using TransformT = YahooBenchmarkSource;

public:
    ptime current_ts;
    YahooBenchmarkSourceEvaluator(int node) : current_ts(boost::gregorian::date(2016, Jan, 1)){
        //current_ts = boost::posix_time::microsec_clock::local_time();
    }

    void evaluate(YahooBenchmarkSource* t, EvaluationBundleContext *c,
                  shared_ptr<BundleBase> bundle_ptr = nullptr) override{
        //output_bundle->add_record(Record<KVPair>(KVPair(str, 1), in.ts));
        auto out = t->getFirstOutput();
        assert(out);

        /* # of bundles between two puncs */
        const uint64_t bundle_per_interval = 1 * numa_num_configured_cpus(); /* # cores */

        boost::posix_time::time_duration punc_interval =
                milliseconds(t->punc_interval_ms);

        boost::posix_time::time_duration delta =
                milliseconds(t->punc_interval_ms) / bundle_per_interval;

        //    const int bundle_count = 2;	// debugging
        const uint64_t records_per_bundle
                = t->records_per_interval / bundle_per_interval;

        EE(" ---- punc internal is %d sec (ev time) --- ",
           t->punc_interval_ms / 1000);

//		const int num_nodes = numa_max_node() + 1;
        const int num_nodes = 1;
        uint64_t us_per_iteration = 1e6 * t->records_per_interval / t->target_tput; /* the target us */
        uint64_t offset = 0;

        while(true){
            boost::posix_time::ptime start_tick = boost::posix_time::microsec_clock::local_time();

            for (unsigned int i = 0; i < bundle_per_interval; i++) {
                /* construct the bundles by reading NUMA buffers round-robin */
                int nodeid = (i % num_nodes);
                shared_ptr<OutputBundleT> //OutputBundleT is RecordBundle<pair<creek::string, long>>;
                        bundle(make_shared<OutputBundleT>(records_per_bundle, nodeid));
                assert(bundle);
                for(unsigned int j = 0; j < records_per_bundle; j++, offset++){
                    if((int64_t)offset == t->record_num){
                        offset = 0; //wrap around
                    }
                    /* rewrite the record ts */
                    t->record_buffers[nodeid][offset].ts = current_ts + delta * i;
#ifdef DEBUG
                    const std::string str_time = to_simple_string(t->record_buffers[nodeid][offset].ts);
					printf("%s \n", str_time);
#endif

#if 0
                    bundle->add_record(
						Record<pair<creek::string, long>>(
							pair<creek::string, long>(
								t->record_buffers[nodeid][offset].data.sd_ip,
								t->record_buffers[nodeid][offset].data.rtt
							), //data: pair
							current_ts + delta * i //ts
						)//Record(data, ts)
					);
#endif
                    bundle->add_record(t->record_buffers[nodeid][offset]);
                }

                out->consumer->depositOneBundle(bundle, nodeid);
                c->SpawnConsumer();
            }//end for


            t->byte_counter_.fetch_add(
                    t->records_per_interval * t->record_len, std::memory_order_relaxed);

            t->record_counter_.fetch_add(t->records_per_interval, std::memory_order_relaxed);

            boost::posix_time::ptime end_tick = boost::posix_time::microsec_clock::local_time();

            auto elapsed_us = (end_tick - start_tick).total_microseconds();
            assert(elapsed_us > 0);

            if ((unsigned long)elapsed_us > us_per_iteration)
                EE("warning: source runs at full speed.");
            else {
                usleep(us_per_iteration - elapsed_us);
                EE("source pauses for %lu us", us_per_iteration - elapsed_us);
            }

            current_ts += punc_interval;
            current_ts += milliseconds(t->session_gap_ms);

            c->UpdateSourceWatermark(current_ts);

            /* Useful before the sink sees the 1st watermark */
            if (c->GetTargetWm() == max_date_time) { /* unassigned */
                c->SetTargetWm(current_ts);
            }

            static int wm_node = 0;
            out->consumer->depositOnePunc(
                    make_shared<Punc>(current_ts, wm_node), wm_node);
            c->SpawnConsumer();

            /*current_ts += punc_interval;
            current_ts += milliseconds(t->session_gap_ms);*/
        }//end while
    }//end evaluate
};// end YahooBenchmarkSourceEvaluator
#endif /* YAHOOBENCHMARKEVALUATOR_H */

#define K2_NO_DEBUG 1

#include "Values.h"
#include "YahooMapperEvaluator.h"
#include "YahooMapper.h"

//template <class InputT, class OutputT, class InputBundleT, class OutputBundleT,
template <class InputT, class OutputT, template<class> class BundleT>
//void WordCountMapper<InputT, OutputT, InputBundleT, OutputBundleT, mode>::ExecEvaluator(int nodeid,
void YahooMapper<InputT, OutputT, BundleT>::ExecEvaluator(int nodeid,
                                                                    EvaluationBundleContext *c, shared_ptr<BundleBase> bundle_ptr)
{
//	using InputBundleT = BundleT<InputT>;
//	using OutputBundleT = BundleT<OutputT>;

#ifndef NDEBUG // if evaluator get stuck ..
    static atomic<int> outstanding (0);
#endif

    /* instantiate an evaluator */
    YahooMapperEvaluator<InputT, OutputT, BundleT> eval(nodeid);

#ifndef NDEBUG
    outstanding ++;
#endif

    eval.evaluate(this, c, bundle_ptr);

#ifndef NDEBUG
    outstanding --;
    int i = outstanding;
    (void)i;
    I("end eval... outstanding = %d", i);
#endif
}

/* ------- template specialization -------
 *
 * NB: we only impl a subset of InputT/OutputT/mode combinations
 *
 * Method cannot do partial template specialization.
 *
 * (tedious...can be more concise?)
 */

template <>
uint64_t YahooMapper<Event, pair<creek::string, long>, RecordBundle>::do_map(Record<Event> const & in, shared_ptr<RecordBundle<pair<creek::string,long>>> output_bundle)
{

    using KVPair = pair<creek::string, long>;

    uint64_t i = 0, cnt = 0;

    if (in.data.num_event_type == 2 ){
        this->itemFromMap = this->campaigns.find(in.data.ad_id);
        if(this->itemFromMap != this->campaigns.end()) {
            output_bundle->add_record(Record<pair<creek::string,long>>(pair<creek::string,long>(creek::string(this->itemFromMap->second),1), in.ts));
            //record_counter_.fetch_add(1, std::memory_order_relaxed);
            return 1;
        }
    }
    return 0;
    //record_counter_.fetch_add(cnt, std::memory_order_relaxed);
    return cnt;
}

template<>
uint64_t YahooMapper<Event, creek::string, RecordBundle>::do_map(Record<Event> const & in, shared_ptr<RecordBundle<creek::string>> output_bundle)
{
    uint64_t i = 0, cnt = 0, start;
    if (in.data.num_event_type == 2 ){
        this->itemFromMap = this->campaigns.find(in.data.ad_id);
        if(this->itemFromMap != this->campaigns.end()) {
            output_bundle->add_record(Record<creek::string>(creek::string(this->itemFromMap->second), in.ts));
            //record_counter_.fetch_add(1, std::memory_order_relaxed);
            return 1;
        }
    }
    //record_counter_.fetch_add(cnt, std::memory_order_relaxed);
    return 0;
}


/* no template specilization */
template <class InputT,
        class OutputT,
        template<class> class BundleT>
atomic<unsigned long> YahooMapper<InputT, OutputT, BundleT>::record_counter_(0);

/* -------instantiation concrete classes------- */

/* using record bundle for input/output */
template
void YahooMapper<Event, creek::string,
        RecordBundle>::ExecEvaluator
        (int nodeid, EvaluationBundleContext *c,
         shared_ptr<BundleBase> bundle = nullptr);

template
void YahooMapper<Event, pair<creek::string, long>,
        RecordBundle>::ExecEvaluator
        (int nodeid, EvaluationBundleContext *c,
         shared_ptr<BundleBase> bundle = nullptr);

/* todo: using record bitmap bundle for input/output */

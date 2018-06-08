#ifndef YAHOOMAPPER_H
#define YAHOOMAPPER_H

#include "Mapper/Mapper.h"
#include "Values.h"

//using namespace boost::uuids;

/*namespace std {
    template <> struct hash<boost::uuids::uuid> {
        size_t operator()(const boost::uuids::uuid& uid) const {
            return boost::hash<boost::uuids::uuid>()(uid);
        }
    };
}*/


/*template <class InputT,
        class OutputT,
        template<class> class BundleT >*/

template <class InputT = Event,
        class OutputT = pair<creek::string, long>,
        template<class> class BundleT = RecordBitmapBundle>
class YahooMapper : public Mapper<InputT> {

//  using OutputBundleT = RecordBitmapBundle<OutputT>;

    using InputBundleT = BundleT<InputT>;
    using OutputBundleT = BundleT<OutputT>;
    using TransformT = YahooMapper<InputT,OutputT,BundleT>;

public:
    static atomic<unsigned long> record_counter_;

private:
    //static atomic<unsigned long> record_counter_;
    unordered_map<creek::string, creek::string> campaigns;
    std::unordered_map<creek::string, creek::string>::iterator itemFromMap;

public:

    YahooMapper(string name = "yahoo_mapper", unordered_map<creek::string, creek::string> campaigns = nullptr) : Mapper<InputT>(name) {
        this->campaigns = campaigns;
    }

    uint64_t do_map(Record<InputT> const & in,
                           shared_ptr<OutputBundleT> output_bundle);

    void ExecEvaluator(int nodeid, EvaluationBundleContext *c,
                       shared_ptr<BundleBase> bundle = nullptr) override;

    bool ReportStatistics(PTransform::Statstics* stat) override {
        /* internal accounting */
        static unsigned long total_records = 0, total_bytes = 0;
        /* last time we report */
        static unsigned long last_bytes = 0, last_records = 0;
        static ptime last_check, start_time;
        static int once = 1;

        /* only care about records */
        total_records = TransformT::record_counter_.load(std::memory_order_relaxed);

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
#if 0
            E("recent: %.2f MB/s %.2f K records/s    avg: %.2f MB/s %.2f K records/s",
  				lmbps, lmrps/1000, mbps, mrps/1000);
#endif

            last_check = now;
            last_bytes = total_bytes;
            last_records = total_records;
        }

//  	E("bundle counter %lu", this->bundle_counter_.load());

        return true;
    }

};
#endif /* YAHOOMAPPER_H */

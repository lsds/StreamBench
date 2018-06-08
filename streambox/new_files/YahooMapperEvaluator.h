#ifndef YAHOOMAPPEREVAULATOR_H
#define YAHOOMAPPEREVAULATOR_H

#include "core/SingleInputTransformEvaluator.h"
#include "Mapper/YahooMapper.h"


template <typename InputT, typename OutputT,
        template<class> class BundleT>
class YahooMapperEvaluator
        : public SingleInputTransformEvaluator<
                YahooMapper<InputT, OutputT, BundleT>, BundleT<InputT>, BundleT<OutputT>>
{

using InputBundleT = BundleT<InputT>;
using OutputBundleT = BundleT<OutputT>;
using TransformT = YahooMapper<InputT, OutputT, BundleT>;

//  using TransformT = WordCountMapper<InputT, OutputT, InputBundleT, OutputBundleT, mode>;
//  using InputBundleT = RecordBitmapBundle<InputT>;
//  using OutputBundleT = RecordBitmapBundle<OutputT>;

public:
bool evaluateSingleInput (TransformT* trans,
                          shared_ptr<InputBundleT> input_bundle,
                          shared_ptr<OutputBundleT> output_bundle) override {

    // go through Records in input bundle (the iterator automatically
    // skips "masked" Records.
    uint64_t cnt = 0;
    for (auto && it = input_bundle->begin(); it != input_bundle->end(); ++it) {
        //        trans->do_map(*it, output_bundle);
        cnt += trans->do_map(*it, output_bundle); /* static rocks! */
    }

    cout << "This mapper produced: " << cnt << " \n";
    trans->record_counter_.fetch_add(cnt, std::memory_order_relaxed);

    return true;
}

    YahooMapperEvaluator(int node)
        : SingleInputTransformEvaluator<TransformT, InputBundleT, OutputBundleT>(node) { }
};

#endif /* YAHOOMAPPEREVAULATOR_H */

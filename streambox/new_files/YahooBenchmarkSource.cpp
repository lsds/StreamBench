#include "Values.h"
#include "YahooBenchmarkSource.h"
#include "YahooBenchmarkSourceEvaluator.h"

void YahooBenchmarkSource::ExecEvaluator(int nodeid, EvaluationBundleContext *c, shared_ptr<BundleBase> bundle_ptr){

    YahooBenchmarkSourceEvaluator eval(nodeid);
    eval.evaluate(this, c, bundle_ptr);
}

#include "Values.h"
#include "UnboundedInMemEvaluator.h"
#include "Unbounded.h"

/* out of line def to resolve dependency. Need to define one for each
 * (partially) specialized class. (string_range and long) */
template<template<class> class BundleT>
void UnboundedInMem<string_range, BundleT>::ExecEvaluator(int nodeid,
		EvaluationBundleContext *c, shared_ptr<BundleBase> bundle_ptr) {
	/* instantiate an evaluator */
	UnboundedInMemEvaluator<string_range,BundleT> eval(nodeid);
	eval.evaluate(this, c, bundle_ptr);
}

template<template<class> class BundleT>
void UnboundedInMem<Event, BundleT>::ExecEvaluator(int nodeid, EvaluationBundleContext *c, shared_ptr<BundleBase> bundle_ptr) {
    /* instantiate an evaluator */
    UnboundedInMemEvaluator<Event,BundleT> eval(nodeid);
    eval.evaluate(this, c, bundle_ptr);
}

/* ---- instantiation ----- */
template
//void UnboundedInMem<string_range, RecordBitmapBundle<string_range>>
void UnboundedInMem<string_range, RecordBitmapBundle>
			::ExecEvaluator(int nodeid,
					EvaluationBundleContext *c,
					shared_ptr<BundleBase> bundle_ptr);

template
//void UnboundedInMem<string_range, RecordBitmapBundle<string_range>>
void UnboundedInMem<string_range, RecordBundle>
			::ExecEvaluator(int nodeid,
					EvaluationBundleContext *c,
					shared_ptr<BundleBase> bundle_ptr);

//hym: for long
template<template<class> class BundleT>
void UnboundedInMem<long, BundleT>::ExecEvaluator(int nodeid, EvaluationBundleContext *c, shared_ptr<BundleBase> bundle_ptr) {/* instantiate an evaluator */
	UnboundedInMemEvaluator<long, BundleT> eval(nodeid);
	eval.evaluate(this, c, bundle_ptr);
}
//hym: instantiation
template
void UnboundedInMem<long, RecordBitmapBundle>::ExecEvaluator(int nodeid, EvaluationBundleContext *c, shared_ptr<BundleBase> bundle_ptr);



//george: for Event
template
void UnboundedInMem<Event, RecordBundle>::ExecEvaluator(int nodeid, EvaluationBundleContext *c, shared_ptr<BundleBase> bundle_ptr);
//george: instantiation
template
void UnboundedInMem<Event, RecordBitmapBundle>::ExecEvaluator(int nodeid, EvaluationBundleContext *c, shared_ptr<BundleBase> bundle_ptr);



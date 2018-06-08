#include "Values.h"
// we must do it here instead of the header

namespace windowed_value {
	GlobalWindow INSTANCE;
	vector<BoundedWindow *> GLOBAL_WINDOWS { &(INSTANCE) };
}

/* avoid ptime underflow problem.
 * (in fact this does not matter?) */
const ptime Window::epoch(boost::gregorian::date(1970, Jan, 1));
//const ptime Window::epoch(boost::gregorian::date(2016, Jan, 1));
//const ptime Window::epoch(min_date_time);

template<>
void WindowsBundle<long>::show_window_sizes() {
#ifndef NDEBUG
	unsigned long sum = 0;
	for (auto&& it = vals.begin(); it != vals.end(); it++) {
		auto output = it->second;
		assert(output);
		EE("window %d (start %s) size: %lu",
				output->id, to_simple_string(output->w.start).c_str(),
				output->vals.size());
		sum += output->vals.size();
		for (auto&& v : output->vals)
			printf("%lu ", v.data);
		printf("\n");
	}
#endif
	//  EE("----- total %lu", sum);
}

#include "core/Transforms.h"

PCollection* PBegin::apply1(PTransform* t) {
	PCollection * p = new PCollection(this->_pipeline); // yes we leak memory...

	t->inputs.push_back(dynamic_cast<PValue *>(this));
	this->consumer = t;

	t->outputs.push_back(dynamic_cast<PValue *>(p));
	p->producer = t;

	return p;
}

vector<PCollection*> PBegin::apply2(PTransform* t) {
	PCollection * out1 = new PCollection(this->_pipeline); // yes we leak memory...
	PCollection * out2 = new PCollection(this->_pipeline); // yes we leak memory...

	t->inputs.push_back(dynamic_cast<PValue *>(this));
	this->consumer = t;

	t->outputs.push_back(dynamic_cast<PValue *>(out1));
	t->outputs.push_back(dynamic_cast<PValue *>(out2));
	out1->producer = t;
	out2->producer = t;

	return vector<PCollection*>({out1, out2});
}

/* xzl: simply making the connection
 * XXX even for Sink, this will create one output PValue. This can be
 * confusing...
 * */
//PCollection* PCollection::apply1(Pipeline *pipeline, PTransform* t) {
PCollection* PCollection::apply1(PTransform* t) {

	t->inputs.push_back(dynamic_cast<PValue *>(this));
	this->consumer = t;

	// only one output from @t is enough
	if (!t->outputs.size()) {
		PCollection * p = new PCollection(this->_pipeline); // yes we leak memory...
		t->outputs.push_back(dynamic_cast<PValue *>(p));
		p->producer = t;
	}

	return dynamic_cast<PCollection*>(t->outputs[0]);
}

/* two outputs */
//vector<PCollection*> PCollection::apply2(Pipeline *pipeline, PTransform* t) {
vector<PCollection*> PCollection::apply2(PTransform* t) {
	PCollection * out1 = new PCollection(this->_pipeline); // yes we leak memory...
	PCollection * out2 = new PCollection(this->_pipeline); // yes we leak memory...

	t->inputs.push_back(dynamic_cast<PValue *>(this));
	this->consumer = t;

	t->outputs.push_back(dynamic_cast<PValue *>(out1));
	t->outputs.push_back(dynamic_cast<PValue *>(out2));
	out1->producer = t;
	out2->producer = t;

	return vector<PCollection*>({out1, out2});
}

#ifdef USE_FOLLY_STRING
/* cf: folly/Hash.h */
namespace tbb {
	size_t tbb_hasher(const folly::fbstring & key) {
		return static_cast<size_t>(
				folly::hash::SpookyHashV2::Hash64(key.data(), key.size(), 0));
	};
}
#endif

namespace creek_set_array {

	void insert(SetArrayPtr ar, creek::string const & in) {
		int index = tbb::tbb_hasher(in) % NUM_SETS;
		//		(ar + index)->insert(in);
		(*ar)[index].insert(in);
	}

	void merge(SetArrayPtr mine, SetArrayPtr const & others) {
		for (int i = 0; i < NUM_SETS; i++) {
			//			(mine + i)->insert((others + i)->begin(), (others + i)->end());
			(*mine)[i].insert((*others)[i].begin(), (*others)[i].end());
		}
	}
}



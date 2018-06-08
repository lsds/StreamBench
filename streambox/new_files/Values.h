#ifndef VALUES_H
#define VALUES_H

#include <typeinfo>
#include <list>
#include <mutex>
#include <memory>
#include <atomic>
#include <unordered_set>
#include <stdio.h>

#include <numaif.h>
#include "utilities/threading.hh"

#include "config.h"
#include "creek-types.h"

#include "AppliedPTransform.h"
// xzl: be careful: this file should not include any other headers that
// contain actual implementation code.

//#include "Transforms.h"
#include "core/Pipeline.h"
#include "WindowingStrategy.h"
#include "log.h"
#include <boost/uuid/uuid.hpp>
#include <boost/uuid/uuid_generators.hpp>
#include <boost/uuid/uuid_io.hpp>

using namespace std;

#ifdef USE_NUMA_ALLOC
#include <numaif.h>
#include "utilities/threading.hh"
using namespace Kaskade;
#endif

/////////////////////////////////////////////////////////////////////////


#ifdef USE_TBB_DS  /* needed for output */
#include "tbb/concurrent_unordered_map.h"
#include "tbb/concurrent_vector.h"
#include "tbb/concurrent_unordered_set.h"
#endif

#ifdef USE_FOLLY_HASHMAP
#include "folly/AtomicHashMap.h"
#endif

#ifdef USE_CUCKOO_HASHMAP
#include "src/cuckoohash_map.hh"
#include "src/city_hasher.hh"
#endif

/////////////////////////////////////////////////////////////////////////

class PValue;
class PTransform;
//class AppliedPTransform<PInput, POutput, PTransform<PInput, POutput> >;

// helper. note: can't return c_str() of a stack string is wrong.
// XXX move to a separate file?
static inline std::string to_simplest_string(const ptime& pt)
{
	const unsigned int skip = 12;
	return to_simple_string(pt).substr(skip);
}

/* ref: http://stackoverflow.com/questions/22975077/how-to-convert-a-boostptime-to-string */
static inline std::stringstream to_simplest_string1(const ptime& pt)
{
	std::stringstream stream;
	boost::posix_time::time_facet* facet = new boost::posix_time::time_facet();
	facet->format("%H:%M:%S%F");
	stream.imbue(std::locale(std::locale::classic(), facet));
	stream << pt;
	return stream;
}

//#ifdef MEASURE_LATENCY
struct mark_entry {
	string msg;
	ptime ts;
	mark_entry(string const & msg, ptime const & ts)
		: msg(msg), ts(ts) { }
};
//#endif

struct bundle_container;

/* does not contain the actual data */
struct BundleBase {

	//hym: which side this bundle/punc belongs to
	// 0: default 
	// 1: left, or left_unordered
	// 2: right, or right_unordered
	int side_info = 0;
	int get_side_info(){
		return side_info;
	}
	void set_side_info(int i){
		side_info = i;
	}


	PValue * value; // the PValue.that encloses the bundle.
	// XXX now only the "Windowed" bundle maintains this
	ptime min_ts = max_date_time;

	int node; // the residing numa node

	atomic<long>* refcnt; /* pointing to the refcnt in the enclosing container */
	bundle_container* container; /* the enclosing container */

	//#ifdef MEASURE_LATENCY
	vector<mark_entry> marks;
	void mark(string const & msg) {
		marks.emplace_back(msg, boost::posix_time::microsec_clock::local_time());
	}

	void inherit_markers(BundleBase const & other) {
		this->marks.insert(this->marks.end(), other.marks.begin(),
				other.marks.end());
	}

	void dump_markers() {
		//#if 0
		int i = 0;
		ptime first;
		ptime last;

		//  	cout << "dump markers: <<<<<<<<<<<<<<<<<<<<<<<<<<<\n";
		cout << "dump markers: <<<<<<<<<<<<<<" << to_simplest_string(this->min_ts) << endl;
		for (auto & e : this->marks) {
			cout << "dump markers:" << i++ << "\t"
				<< e.msg << " ";

			if (i == 1) {
				first = e.ts;
				cout << to_simple_string(e.ts) << endl;
			}
			else {
				//  			cout << to_simple_string(e.ts);
				cout << " +" << (e.ts - last).total_milliseconds() << " ms" << endl;
			}

			last = e.ts;
            cout << to_simple_string(e.ts) << endl;
		}
		cout << "dump markers: >>>>>>>>>>>>>>>>>>>>>>>>>>>>";
		cout << "total " << (last - first).total_milliseconds() << " ms" << endl;
		//#endif
	}
	//#endif

	/* must be explicit on node */
	BundleBase(int node) : value(nullptr), node(node) { }

	virtual ~BundleBase() {
		//  	VV("bundle destroyed");
	}

	/* how many records in the bundle? useful if the bundle class has
	 * linear d/s internally.
	 * -1 means the bundle class does not implement the method.
	 */
	virtual int64_t size() { return -1; }
};

/* punctuation. reuse min_ts as the carried upstream wm snapshot. */
struct Punc : public BundleBase {
	Punc(ptime const & up_wm, int node = -1) : BundleBase(node) {
		this->min_ts = up_wm;
		this->staged_bundles = nullptr;
	}

	/* bundle staged in the container which this punc is assigned to. before
	 * depositing the punc to downstream, these bundles must be deposited first.
	 */
	vector<shared_ptr<BundleBase>>* staged_bundles;

};

/* container open */
#define PUNCREF_UNDECIDED 			(-1)
#define PUNCREF_CANCELED		 		(-2)  /* container sealed */
#define PUNCREF_MAX							3     /* no actual meaning */
#define PUNCREF_ASSIGNED 				2			/* container sealed */
#define PUNCREF_RETRIEVED 			1			/* container sealed */
#define PUNCREF_CONSUMED 				0			/* container sealed */

/* order must be consistent with above */
#if 0
static const char * puncref_desc_[] = {
	"canceled",
	"undecided",
	"consumed",
	"retrieved",
	"assigned",
	""
};
#endif

static const char * puncref_key_[] = {
	"X",
	"U",
	"C",
	"R",
	"A",
	""
};

#define SIDE_INFO_NONE 		0  /* default. single input */
#define SIDE_INFO_L 			1
#define SIDE_INFO_R 			2
#define SIDE_INFO_J 			3  /* join */
#define SIDE_INFO_JD 			4  /* join's downstream */
#define SIDE_INFO_JDD 		5  /* join's downstream downstream */

const int puncref_desc_offset = 2;

#define puncref_desc(x) (puncref_desc_[x + puncref_desc_offset])
#define puncref_key(x) (puncref_key_[x + puncref_desc_offset])


/* future ideas:
 *  - @bundles can use some concurrent d/s where adding is lockfree
 *  - make @bundles lockfree and make @punc atomic (enforce a fence
 * around it)
 */
struct bundle_container {

	public:
		//hym: some trans need to know where bundles/puncs come from
		//     left? right? unordered? ordered? 
		atomic<int> side_info;
		void set_side_info(int i){
			side_info = i;
		}

		int get_side_info(){
			return side_info.load();
		}

		int has_punc = 0; //0: this container doesn't have pun cyet
		//1: this container already has a punc

		shared_ptr<Punc> punc; /* unsafe to access this from outside w/o protection */

	public:
		/* # of deposited, but not yet consumed, bundles.
		 * atomic because we allow bundle consumers to decrement this w/o locking
		 * containers. */
		atomic<long> refcnt;

		/* punc 	 punc_refcnt
		 * ---------------------
		 * nullptr 	  -2				punc invalidated by upstream (i.e. will never be assigned)
		 * nullptr		-1				punc never assigned
		 * valid			2					assigned, not emitted (== retrieved)
		 * valid			1			  	assigned, emitted, not consumed
		 * valid			0					consumed
		 */
		atomic<long> punc_refcnt;


		/* Can be a variety of d/s. Does not have to be ordered.
		 * the concurrent version still needs lock for erase -- unsafe */
		//  	mutex mtx;
		//  	unordered_set<shared_ptr<BundleBase>> bundles;
		vector<shared_ptr<BundleBase>> bundles;
		//	list<shared_ptr<BundleBase>> bundles;

		std::mutex mtx_;  /* container-wide lock */

		/* OBSOLETED:
		 * Staged bundles are output produced by processing bundles in this container;
		 * however, if there is an earlier container which may assign a punc to
		 * downstream later, depositing these bundles to downstream at this time.will
		 * enable them overtake the punc, which will incur punc delay.
		 *
		 * Instead, we stage them here. Once the earlier container becomes dead and
		 * is cleaned, we flush the staged bundles to downstream.
		 */
		vector<shared_ptr<BundleBase>> staged_bundles;

	public:
		/* the container for output bundles. Has to be atomic pointer since a thread
		 * may open the downstream pointer and link to it. */
		//	bundle_container *downstream;
		atomic<bundle_container*> downstream;

	public:
		//	bundle_container () : punc(nullptr), bundles(), punc_refcnt(-1), refcnt(0) { }
		//		bundle_container () : punc_refcnt(-1), punc(nullptr), refcnt(0) { }
		/* xzl: the only workarond...why? */
		bundle_container (int side_info = 0)
			//: punc(nullptr), downstream(nullptr), side_info(side_info)
			: side_info(side_info), punc(nullptr), downstream(nullptr) //fix warning
		{
			//side_info = 0;
			punc_refcnt = PUNCREF_UNDECIDED; refcnt = 0;
			//		bundles.reserve(80);  // for vector. actually make things slightly worse.
		}

		/* for stat */
		unsigned long peekBundleCount(void) {
			return bundles.size();  /* is this okay? */
		}

	private:
		/* the following are unsafe: need lock, unless we already w locked all
		 * containers of a trans.
		 */

		/* get and erase one bundle. order does not matter.
		 * unsafe. need lock, unless we already locked all containers of a trans.
		 * return: nullptr if no bundle to return. */
		shared_ptr<BundleBase> getBundleUnsafe(void) {
			shared_ptr<BundleBase> ret;
			if (bundles.empty())
				return nullptr;

			assert(this->refcnt); /* > 0 since at least have one bundle */

			ret = bundles.back();
			bundles.pop_back();
			return ret;
		}

		void putBundleUnsafe(shared_ptr<BundleBase> const & bundle) {
			/* increment refcnt before depositing, so that when a consumer
			 * consumes the bundle, it always
			 * sees a legit refcnt in decrementing the refcnt. */
			auto oldref = this->refcnt.fetch_add(1);
			xzl_assert(oldref >= 0);
			//		if(oldref < 0){
			//			EE("oldref is %ld < 0", oldref);
			//			abort();
			//		}

			bundle->refcnt = &(this->refcnt);
			/* atomic implies mem fence here */
			bundle->container = this;
			/* XXX make ->container atomic? */
			this->bundles.push_back(bundle);
		}

		/* assign a punc to this container.
		 * By pipeline design, there's no concurrent punc writer.
		 * However, there could be concurrent reader/writer of punc.
		 */
		void setPuncUnsafe(shared_ptr<Punc> const & punc) {
			assert(punc_refcnt != 0 && "can't do so on dead container");
			assert(punc_refcnt != 1 && "can't overwrite an emitted punc");
			assert(punc_refcnt <= 2);

			if (!this->punc)
				assert(punc_refcnt == -1);

			punc->refcnt = &(this->punc_refcnt);
			punc->container = this;
			punc->staged_bundles = &(this->staged_bundles);

			this->punc_refcnt = PUNCREF_ASSIGNED; /* see comment above */
			this->punc = punc;
		}

	public:
		/*
		 * The MT Safe verion is necessary: some execution paths, e.g.
		 * depositBundlesToContainer(), since it already knows the source/dest
		 * containers, no longer locks all containers of a trans.
		 *
		 * Therefore, these paths must lock individual containers.
		 */

		shared_ptr<BundleBase> getBundleSafe(void) {
			unique_lock<mutex> lck(this->mtx_);
			return getBundleUnsafe();
		}

		void putBundleSafe(shared_ptr<BundleBase> const & bundle) {
			unique_lock<mutex> lck(this->mtx_);
			putBundleUnsafe(bundle);
		}

		/* get/set Punc should be locked since @punc is not atomic (does not
		 * imply a fence)
		 */
		void setPuncSafe(shared_ptr<Punc> const & punc) {
			unique_lock<mutex> lck(this->mtx_);
			setPuncUnsafe(punc);
		}

		shared_ptr<Punc> const & getPuncSafe(void) {
			unique_lock<mutex> lck(this->mtx_);
			return punc;
		}

		/* check the invariant between punc_refcnt and punc.
		 * have to lock to enforce consistency for @punc (not aotmic) and
		 * @punc_refcnt */
		bool verifyPuncSafe() {
			unique_lock<mutex> lck(this->mtx_);
			if (this->punc_refcnt == PUNCREF_UNDECIDED && this->punc) {
				return false;
			}
			return true;
		}
};

#define MAX_NUMA_NODES	8
#define ASSERT_VALID_NUMA_NODE(node) do { 			\
	assert(node >= 0 && node <MAX_NUMA_NODES);		\
} while (0)

/**
 * The interface for values that can be input to and output from {@link PTransform PTransforms}.
 */
class PValue {

	using bundle_queue_t = deque<shared_ptr<BundleBase>>;

	public:
	virtual Pipeline* getPipeline() = 0;
	virtual string getName() = 0;
	virtual ~PValue() { };

	/**
	 * Expands this {@link POutput} into a list of its component output
	 * {@link PValue PValues}.
	 *
	 * <ul>
	 *   <li>A {@link PValue} expands to itself.</li>
	 *   <li>A tuple or list of {@link PValue PValues} (such as
	 *     {@link PCollectionTuple} or {@link PCollectionList})
	 *     expands to its component {@code PValue PValues}.</li>
	 * </ul>
	 *
	 * <p>Not intended to be invoked directly by user code.
	 */
	virtual list<PValue *>* expand() = 0;

	// input only
	virtual void finishSpecifying() = 0;

	// output only. nop for input.
	virtual void recordAsOutput(AppliedPTransform* transform) { }
	virtual void finishSpecifyingOutput() { }
	virtual AppliedPTransform* getProducingTransformInternal() { return NULL; }

	PTransform *producer = nullptr;
	PTransform *consumer = nullptr;

	/* @node:
	 * >= 0 specific numa nodes
	 * -2 any numa node would do (e.g. for join)
	 * -1 unspecified (error)
	 */
	shared_ptr<BundleBase> getOneBundle(int node = -1) {

		assert(node != -1);

#ifdef INPUT_ALWAYS_ON_NODE0
		node = 0;
#endif

		unique_lock <mutex> lock(_bundle_mutex);

		if (node == -2) {
			for (int n = 0; n < MAX_NUMA_NODES; n ++)
				if (!numa_bundles[n].empty()) {
					node = n;
					break;
				}
		}
		/* if all empty, node == -2 */

		auto & bundles = numa_bundles[node];

		if (bundles.empty() || node == -2) {
			/* debugging */
			EE("%s: bug? : bundles %u %u %u %u; min_ts %s",
					getName().c_str(),
					(unsigned int)(numa_bundles[0].size()),
					(unsigned int)(numa_bundles[1].size()),
					(unsigned int)(numa_bundles[2].size()),
					(unsigned int)(numa_bundles[3].size()),
					to_simplest_string(min_ts).c_str());

			return nullptr;
		} else {
			shared_ptr<BundleBase> ret = bundles.back();
			bundles.pop_back();

			/* xzl: find min_ts among the remaining bundles.
			 * expensive? */
			min_ts = max_date_time;
			for (auto& q : numa_bundles) {
				for (auto& b : q) {
					if (b->min_ts < min_ts)
						min_ts = b->min_ts;
				}
			}

#ifndef INPUT_ALWAYS_ON_NODE0
			assert(ret->node == node);
			//#else /* we don't touch the nodeid saved in bundle */
			//			assert(ret->node == 0);
#endif

#if 0 /* debugging */
			EE("%s: --- okay --- : bundles %u %u %u %u; min_ts %s",
					getName().c_str(),
					(unsigned int)(numa_bundles[0].size()),
					(unsigned int)(numa_bundles[1].size()),
					(unsigned int)(numa_bundles[2].size()),
					(unsigned int)(numa_bundles[3].size()),
					to_simplest_string(min_ts).c_str());
#endif

			return ret;
		}
	}

	// @node: -1 does not care
	// after this, the bundle is owned by the downstream transform
	void depositOneBundle(shared_ptr<BundleBase> bundle, int node = -1) {
		assert(bundle);

#ifdef INPUT_ALWAYS_ON_NODE0
		/* we enqueue to bundle to node0, w/o changing the nodeid saved in
		 * the bundle, which will be used to determine task home node. */
		node = 0;
		//    bundle->node = 0;
#endif

		if (node == -1)
			node = bundle->node; /* best effort determining the dest node */

		ASSERT_VALID_NUMA_NODE(node);

		/* sanity check */
#ifndef INPUT_ALWAYS_ON_NODE0
#if 1
		if (bundle->node != node)
			EE("bundle->node %d node %d", bundle->node, node);
#endif
		assert(bundle->node == node);
#endif

		unique_lock <mutex> lock(_bundle_mutex);

		auto & bundles = numa_bundles[node];

		bundle->value = this;
		bundles.push_front(bundle);
		if (bundle->min_ts < min_ts)
			min_ts = bundle->min_ts;

		W("%s: bundle %s, after: bundles %u %u %u %u; min_ts %s",
				getName().c_str(),
				to_simplest_string(bundle->min_ts).c_str(),
				(unsigned int)(numa_bundles[0].size()),
				(unsigned int)(numa_bundles[1].size()),
				(unsigned int)(numa_bundles[2].size()),
				(unsigned int)(numa_bundles[3].size()),
				to_simplest_string(min_ts).c_str());
	}

	/* return total # of bundles. peek w/o lock.
	 * -1: return the sum of bundles on all nodes */
	int size(int node = -1) {

		assert(node < MAX_NUMA_NODES);

		int sz = 0;
		if (node == -1) {
			for (auto & b : numa_bundles)
				sz += b.size();
			return sz;
		} else
			return numa_bundles[node].size();
	}

	// the ts of the oldest record across all bundles (across all nodes)
	ptime min_ts;

	protected:
	/* so far one lock for all NUMA queues. this is because each deposit/get
	 * action will have to update min_ts, which needs to lock all queues anyway.
	 * XXX revisit if the # of bundles becomes high
	 */
	mutex _bundle_mutex;
	//  mutex _bundle_mutex[MAX_NUMA_NODES];

	public:
	/* existing Bundles to be consumed (by the next transform)
	 * XXX the queues are accessed from different core/nodes. do cacheline
	 * alignment
	 * XXX using lockfree d/s
	 */
	bundle_queue_t numa_bundles[MAX_NUMA_NODES];

#if 0
	PValue() {
		for (auto & t : min_ts) {
			t = max_date_time;
		}
	}
#endif
};

////////////////////////////////////////////////////////////////////////////

class PValueBase : public PValue {
	public:
		Pipeline* getPipeline() {
			return _pipeline;
		}

		AppliedPTransform* getProducingTransformInternal() {
			return _producingTransform;
		}

		void recordAsOutput (AppliedPTransform* transform) {

		}

		void finishSpecifyingOutput() { }

		// return a copy
		string getName() {
			//	  assert(!_name.empty()); return _name;
			if (_name.empty())
				_name = "[" + string(typeid(*this).name()) + "]";
			return _name;
		}

		// create a copy
		void setName(string name) { assert(!_finishedSpecifying); this->_name = name; }

		bool isFinishedSpecifyingInternal() {
			return _finishedSpecifying;
		}

		void finishSpecifying() {
			_finishedSpecifying = true;
		}

		string toString() {
			return (_name.empty() ? "<unamed>"
					: getName() + "[" + getKindString() +"]");
		}

		list<PValue *>* expand() { return new list<PValue *>({this}); }

	protected:
		Pipeline* _pipeline;
	public:
		string _name;
	private:
		AppliedPTransform* _producingTransform ;
		bool _finishedSpecifying;

	protected:

		PValueBase(Pipeline *pipeline, string name)
			: _pipeline(pipeline), _name(name), _producingTransform(NULL),
			_finishedSpecifying(false) { }

		PValueBase(Pipeline *pipeline)
			: _pipeline(pipeline), _producingTransform(NULL),
			_finishedSpecifying(false) { }

		void recordAsOutput(AppliedPTransform* t, const string& outName) {
			if (_producingTransform) // already have one producing transform
				return;

			_producingTransform = t;

			if (_name.empty()) {
				_name = t->getFullName() + "." + outName;
			}
		}

		string getKindString() {
			// TODO: see Beam impl.
			return string(typeid(*this).name());
		}
};


class PDone : PValueBase {
	private:
		PDone(Pipeline* pipeline) : PValueBase(pipeline, "done") {
		}

	public:
		static PDone* in(Pipeline* pipeline) {
			return new PDone(pipeline);
		}

		string getName() {
			return "pdone";
		}

		// A PDone contains no PValues.
		list<PValue *>* expand() { return new list<PValue *>(); }

		void finishSpecifyingOutput() {}
};

////////////////////////////////////////////////////////////////////////////

class WindowingStrategy;

struct BundleBase;

class PCollection : public PValueBase {
	public:
		// xzl: c++11 feature: scoped enum
		enum class IsBounded {
			BOUNDED,
			UNBOUNDED
		};

		IsBounded And(IsBounded that) {
			if (this->_isBounded == IsBounded::BOUNDED
					&& that == IsBounded::BOUNDED)
				return IsBounded::BOUNDED;
			else
				return IsBounded::UNBOUNDED;
		}

		string getName() { return PValueBase::getName(); }

		// return a ref
		PCollection* setName(string name) { PValueBase::setName(name); return this; }

#if 1
		PValue* apply(string name, PTransform* t) {
			return Pipeline::applyTransform(name, this, t);
		}

		PValue* apply(PTransform* t) { return Pipeline::applyTransform(this, t); }
#endif

		/* xzl: simply making the connection */
		PCollection* apply1(PTransform* t);

		vector<PCollection*> apply2(PTransform* t);

		IsBounded isBounded() { return _isBounded; }

		WindowingStrategy* getWindowingStrategy() {
			return _windowingStrategy;
		}

		PCollection* setWindowingStrategyInternal(WindowingStrategy* s) {
			this->_windowingStrategy = s;  // overwrite
			return this;
		}

		PCollection* setIsBoundedInternal(IsBounded isBounded) {
			this->_isBounded = isBounded;
			return this;
		}

#if 0
		static PCollection<T>* createPrimitiveOutputInternal(Pipeline* p, WindowingStrategy* s, IsBounded isBounded) {
			PCollection<T> *c = new PCollection<T> (p);
			return c->setWindowingStrategyInternal(s)->setIsBoundedInternal(isBounded);
		}
#endif

		static PCollection* createPrimitiveOutputInternal(Pipeline* p) {
			PCollection *c = new PCollection (p);
			return c;
		}

		list<PValue *>* expand() { return new list<PValue *>({this}); }

		// needed as PInput
		Pipeline* getPipeline() { return PValueBase::getPipeline(); }

		// needed as PInput
		void finishSpecifying() {
			PValueBase::finishSpecifying();  // as POutput
			// TODO: as input??
		}

#if 0
		BundleBase* getOneBundle() {
			unique_lock <mutex> lock(_bundle_mutex);
			if (bundles.empty())
				return nullptr;
			else {
				BundleBase* ret = bundles.back();
				bundles.pop_back();
				return ret;
			}
		}

		void depositOneBundle(BundleBase* bundle) {
			assert(bundle);
			unique_lock <mutex> lock(_bundle_mutex);
			bundles.push_front(bundle);
			bundle->collection = this;
		}
#endif

	private:
		IsBounded _isBounded;
		WindowingStrategy* _windowingStrategy;

	public:
		PCollection(Pipeline* p)
			: PValueBase(p),
			_isBounded(IsBounded::BOUNDED),
			_windowingStrategy(NULL)
	{ }

		PCollection(Pipeline* p, string name)
			: PValueBase(p, name),
			_isBounded(IsBounded::BOUNDED),
			_windowingStrategy(NULL)
	{ }
};

////////////////////////////////////////////////////////////////////////////

class PBegin : public PValueBase {
	public:
		static PBegin* in(Pipeline *p) {
			return new PBegin(p);
		}

		PValue* apply(PTransform *p) { return Pipeline::applyTransform(this, p); }

		PValue* apply(string name, PTransform *p) { return Pipeline::applyTransform(name, this, p); }

		PCollection* apply1(PTransform *p);

		vector<PCollection*> apply2(PTransform* t);

		void finishSpecifying() { }

		list<PValue *>* expand() { return new list<PValue *>(); }

		string getName() { return "pbegin"; }

	protected:
		PBegin(Pipeline *p) : PValueBase(p, "begin") { }
};

////////////////////////////////////////////////////////////////////////////

/**
 * An immutable pair of a value and a timestamp.
 *
 * <p>The timestamp of a value determines many properties, such as its assignment to
 * windows and whether the value is late (with respect to the watermark of a {@link PCollection}).
 *
 * @param <V> the type of the value
 */

#include "boost/date_time/posix_time/ptime.hpp"
using namespace boost::posix_time;

template <class V>
class TimestampedValue {
	public:
		// xzl: note that we return value by copy.
		static TimestampedValue<V> of(V value, ptime timestamp) {
			return TimestampedValue<V>(value, timestamp);
		}

		V getValue() { return _value; }

		ptime getTimestamp() { return _timestamp; }

		bool equals(const TimestampedValue<V>& other) {
			return ((_value == other._value) && (_timestamp == other._timestamp));
		}

		string toString() {
			// xzl: XXX?
			return "TimestampedValue(" + to_string(_value) + \
				", " + "XXX to_string(_timestamp)" + ")";
		}
	private:
		V _value;
		ptime _timestamp;
};

////////////////////////////////////////////////////////////////////////////

#include <vector>
#include "BoundedWindow.h"
#include "PaneInfo.h"

using namespace pane_info;

namespace windowed_value {
	extern vector<BoundedWindow *> GLOBAL_WINDOWS;
	//  vector<BoundedWindow *> GLOBAL_WINDOWS { &(GlobalWindow::INSTANCE) };
	//  vector<BoundedWindow *> GLOBAL_WINDOWS { &(global_window::INSTANCE) };
}

template <class T>
class WindowedValue {
	protected:
		T 			_value;
		PaneInfo 	_pane;

	public:

		WindowedValue(T value, PaneInfo pane) : _value(value), _pane(pane) { }
};

////////////////////////////////////////////

/**
 * The abstract superclass of WindowedValue representations where
 * timestamp == MIN.
 */

template <class T>
class MinTimestampWindowedValue : public WindowedValue<T> {
	public:
		MinTimestampWindowedValue(T value, PaneInfo pane)
			: WindowedValue<T>(value, pane) { }

		ptime getTimestamp() {
			return BoundedWindow::TIMESTAMP_MIN_VALUE;
		}
};

////////////////////////////////////////////

/**
 * The representation of a WindowedValue where timestamp == MIN and
 * windows == {GlobalWindow}.
 */

template <class T>
class ValueInGlobalWindow : public MinTimestampWindowedValue<T> {
	public:
		ValueInGlobalWindow(T value, PaneInfo pane)
			: MinTimestampWindowedValue<T>(value, pane) { }

		// xzl: upcasting?
		WindowedValue<T>* withValue(T value) {
			return (WindowedValue<T>*) new ValueInGlobalWindow(value, this->_pane);
		}

		vector<BoundedWindow *>* getWindows() {
			return &(windowed_value::GLOBAL_WINDOWS);
		}
		// todo: equals?
};

////////////////////////////////////////////

/**
 * The representation of a WindowedValue where timestamp == MIN and
 * windows == {}.
 */
template <class T>
class ValueInEmptyWindows : public MinTimestampWindowedValue<T> {
	public:
		ValueInEmptyWindows(T value, PaneInfo pane)
			: MinTimestampWindowedValue<T>(value, pane) { }

		// xzl: upcasting?
		WindowedValue<T>* withValue(T value) {
			return (WindowedValue<T>*) new ValueInEmptyWindows<T>(value, this->_pane);
		}

		vector<BoundedWindow *>* getWindows() {
			return new vector<BoundedWindow *> { };   // empty
		}
};

////////////////////////////////////////////

// xzl: values that have *actual* ts (not just MIN)
template <class T>
class TimestampedWindowedValue : public WindowedValue<T> {
	protected:
		ptime _ts;

	public:
		TimestampedWindowedValue(T value, ptime ts, PaneInfo pane)
			: WindowedValue<T>(value, pane), _ts(ts) { }

		ptime getTimestamp() { return _ts; }
};

////////////////////////////////////////////

template <class T>
class TimestampedValueInMultipleWindows : public TimestampedWindowedValue<T> {
	private:
		vector<BoundedWindow *>* _windows;

	public:
		TimestampedValueInMultipleWindows(T value, ptime ts,
				vector<BoundedWindow *>* windows, PaneInfo pane)
			: TimestampedWindowedValue<T>(value, ts, pane), _windows (windows) {
				assert (_windows);
			}

		// xzl: upcasting?
		WindowedValue<T>* withValue(T value) {
			return (WindowedValue<T>*)
				new TimestampedValueInMultipleWindows<T>(value,
						this->_ts, _windows, this->_pane);
		}

		vector<BoundedWindow *>* getWindows() {
			return _windows;
		}
};

// xzl: we need to extract these funcs from WindowedValue, since they do
// forward reference
namespace windowed_value {
	//  vector<BoundedWindow *> GLOBAL_WINDOWS { &(GlobalWindow::INSTANCE) };

	using namespace pane_info;

	template <class T>
		static WindowedValue<T>* of(T value, ptime ts,
				vector<BoundedWindow *>* windows, PaneInfo pane) {
			if (windows->empty() && BoundedWindow::TIMESTAMP_MIN_VALUE == ts){
				return new ValueInEmptyWindows<T>(value, *(pane_info::NO_FIRING));
			} else if (windows->size() == 1) {
				return of(value, ts, windows[0], pane);
			} else
				return new TimestampedValueInMultipleWindows<T>(value, ts,
						windows, pane);
		}
}

// xzl: simplified version of each element (record)

// a record with two data fields
template <typename T1, typename T2>
struct Element2 {
	using Tuple = std::tuple<T1,T2>;

	Tuple cols;
	ptime ts;
	ptime ts2;
	BoundedWindow * window;

	Element2 (Tuple cols,
			ptime ts = max_date_time,
			ptime ts2 = max_date_time,
			BoundedWindow * window = nullptr
		 ): cols(cols), ts(ts), ts2(ts2), window(window) { }

};

template <typename T1, typename T2>
struct Header2 {
	typedef std::tuple<vector<T1>*, vector<T2>*> Tuple;
	vector<bool>* selector = nullptr;
	vector<ptime>* ts = nullptr;
	vector<ptime>* ts2 = nullptr;
	Tuple cols;
	BoundedWindow * window = nullptr; // is this okay?
};


// a part of a PCollection. elements stored in a vector
// XXX add timestamp and window to each element?

template <typename T>
struct Record {

	using ElementT = T;

	ptime ts;
	T data;

	Record(const T& data, const ptime& ts)
		: ts(ts), data(data) { }

	// no ts provided.
	Record(const T& data) : ts(min_date_time), data(data) { }

	// we seem to need this, as container may initialize a blank record?
	Record() { }
};

struct Window;

// row format.
// @T: element type, e.g. long
#ifdef USE_FOLLY_VECTOR
#include "folly/FBVector.h"  /* if using folly... */
#endif
template <typename T>
struct RecordBundle : public BundleBase {
	using RecordT = Record<T>;

	using AllocatorT = creek::allocator<RecordT>;

#ifdef USE_FOLLY_VECTOR
	using VecT = folly::fbvector<RecordT, AllocatorT>;
#else
	using VecT = vector<RecordT, AllocatorT>;
#endif

	//  static NumaAllocator<T> _alloc;
	//  NumaAllocator<RecordT> alloc;

	VecT content;

	int64_t size() override { return content.size(); }

	// minimize reallocation
	RecordBundle(int init_capacity = 1024, int node = -1)
#ifdef USE_NUMA_ALLOC
		: BundleBase(node) , content(AllocatorT(node))
#else
		: BundleBase(node)
#endif

		{
			assert(init_capacity >= 0);
			content.reserve(init_capacity);
		}

	// has to maintain per bundle ts
	virtual void add_record(const RecordT& rec) {
		content.push_back(rec);
		if (rec.ts < min_ts)
			min_ts = rec.ts;
	}

	/* only for specialized */
	void add_record(const Window & win, const RecordT& rec);

	virtual void emplace_record(const T& v, ptime const & ts) {
		content.emplace_back(v, ts);
		if (ts < min_ts)
			min_ts = ts;
	}

	//  typename vector<RecordT>::iterator begin() {
	//  typename decltype(content)::iterator begin() {
	typename VecT::iterator begin() {
		return content.begin();
	}

	//  typename vector<RecordT>::iterator end() {
	typename VecT::iterator end() {
		return content.end();
	}

#if 0
	/* for debugging */
	virtual ~RecordBundle() {
		W("bundle destroyed. #records = %lu", _content.size());
	}
#endif
};

/* specialization (inline).
 * ignore the @win info. to be compatible with the WindowsBundle interface */
template<> inline
void RecordBundle<creek::tvpair>::add_record(const Window & win, const RecordT& rec) {
	add_record(rec);
}

template <typename T>
struct RecordBundleDebug : public RecordBundle<T> {
	/* for debugging */
	virtual ~RecordBundleDebug () {
		W("bundle destroyed. #records = %lu", this->content.size());
	}
};

/* has a bitmap that supports indicate "masked/deleted" records.
 * provides an iterator to transparently go over the unmasked Records.
 */
template <typename T>
struct RecordBitmapBundle : public RecordBundle<T>
{
	using RecordT = Record<T>;

	//#ifdef USE_NUMA_ALLOC
	//  using AllocatorT = NumaAllocator<bool>;
	//#else
	//  using AllocatorT = std::allocator<bool>;
	//#endif

	using AllocatorT = creek::allocator<bool>;

	vector<bool, AllocatorT> selector;
	//vector<bool> selector;

	//////////////////////////////////////////////////////////////
	// cannot use "const" iterator, since we have to modify the bitmap
	class iterator {
		private:
			uint64_t index;
			RecordBitmapBundle *bundle;

		public:
			iterator(RecordBitmapBundle* b, uint64_t index)
				: index(index), bundle(b) {
					// only can be constructed from begin and end.
					// since any where in the middle may hit "holes" and we don't
					// deal with that.
					assert(index == bundle->content.size() || index == 0);

					// if this is begin, seek the next unmasked item
					while (this->index < bundle->content.size()
							&& !bundle->selector[this->index])
						this->index ++;
				}

			bool operator !=(iterator const& other) const {
				return index != other.index;
			}

			// find the next unmasked item
			iterator& operator++() {
				if (index < bundle->content.size()) {
					index ++;
					while (index < bundle->content.size() && !bundle->selector[index])
						index ++;
				}
				return *this;
			}

			// will this incur extra copy?
			RecordT const& operator*() {
				//    RecordT & operator*() {
				return bundle->content[index];
			}

			bool mask() {
				assert(index >= 0 && index < bundle->content.size()
						&& index < bundle->selector.size());

				bundle->selector[index] = false;
				return true; // XXX gracefully
			}

			// ever needed?how
			bool unmask() {
				assert(index >= 0 && index < bundle->content.size()
						&& index < bundle->selector.size());

				bundle->selector[index] = true;
				return true; // XXX gracefully
			}

			};
			//////////////////////////////////////////////////////////////

			// XXX make the following two consts

			iterator begin() {
				return iterator(this, 0);
			}

			iterator end() {
				return iterator(this, this->content.size());
			}

			RecordBitmapBundle(int init_capacity = 128, int node = -1)
				: RecordBundle<T>(init_capacity, node)
#ifdef USE_NUMA_ALLOC
				  , selector(AllocatorT(node))
#endif
			{
				// {
				selector.reserve(init_capacity);
			}

			virtual void add_record(const RecordT& rec) override {
				selector.push_back(true);
				RecordBundle<T>::add_record(rec);
			}

			void print(ostream &os) const {
				int count = 0, max_count = 3;
				os << "dump RecordBitmapBundle: ";
				for (auto it = this->begin(); it != this->end(); it++) {
					if (count ++ >= max_count)
						break;
					os << it->data << " ";
				}
				os << endl;
			}
			};

			// col format. need to specify the type of each column
			// Each column is a shared_ptr, as columns may be shared across bundles.
			template <typename T1, typename T2>
				struct BundleCol2 : public BundleBase
			{
				typedef std::tuple<shared_ptr<vector<T1>>, shared_ptr<vector<T2>>> Tuple;

				// make them pointers to enable sharing across bundles.
				//  vector<bool>* selector = nullptr;
				shared_ptr<vector<bool>> selector = nullptr;
				shared_ptr<vector<ptime>> ts = nullptr;
				shared_ptr<vector<ptime>> ts2 = nullptr;

				Tuple cols;

				BoundedWindow * window = nullptr; // is this okay?

				BundleCol2(int node = -1): BundleBase(node) { }

			};

			////////////////////////////////////////////////////////////////////////

			/* we need to hash this object
			 * ref: http://stackoverflow.com/questions/1574647/a-way-to-turn-boostposix-timeptime-into-an-int64 */
#if 0
			template <unsigned int duration_ms>
				struct Window {
					//  ptime start;
					boost::posix_time::time_duration start; // window start
					const static boost::posix_time::time_duration duration; // not needed if we do fixed window
					const static ptime epoch; // the start of our time
				};

			template <unsigned int duration_ms>
				const boost::posix_time::time_duration
				Window<duration_ms>::duration = milliseconds(duration_ms);

			template <unsigned int duration_ms>
				const ptime
				Window<duration_ms>::epoch = boost::gregorian::date(1970, Jan, 1);
#endif

			struct Window {
				//  ptime start;
				boost::posix_time::time_duration start; // window start
				// cannot be const, since it'll block the implict copy function
				// its value is not used in sorting

				// the length of the window. not needed if we do fixed window
				mutable boost::posix_time::time_duration duration;
				const static ptime epoch; // the start of our time

				Window (ptime start, boost::posix_time::time_duration const & duration)
					: start(start - epoch), duration(duration) { }

				// dummy ctor -- just do nothing
				Window () { }

				inline ptime window_start() const {
					return epoch + start;
				}

				inline ptime window_end() const {
					return epoch + start + duration;
				}

				// assuming unified sized window, we only compare their start time.
				// this enables Window as key in std::map.
#if 0
				bool operator()(const Window*& lhs, const Window*& rhs) const {
					return lhs->start < rhs->start;
				}
#endif
				bool operator()(const Window& lhs, const Window& rhs) const {
					return lhs.start < rhs.start;
				}
			};

			/* one struct. needed if we use tbb hash table.
			 * see https://www.threadingbuildingblocks.org/docs/help/tbb_userguide/More_on_HashCompare.html */
			struct WindowHashCompare {
				static size_t hash(const Window & w) {
					return (size_t)(w.duration.ticks());
				}

				static bool equal(const Window& lhs, const Window& rhs) {
					return (lhs.start == rhs.start && lhs.duration == rhs.duration);
				}
			};

			/* needed for (tbb) unordered map */
			struct WindowHash {
				size_t operator()(const Window & w)  const {
					return (size_t)(w.duration.ticks());
				}
			};

			struct WindowEqual {
				bool operator()(const Window& lhs, const Window& rhs) const {
					return (lhs.start == rhs.start && lhs.duration == rhs.duration);
				}
			};

			// window that facilitates on-the-fly merge
			// @T: the element type, e.g. long
			template<typename T>
				struct SessionWindow : public Window {
					using RecordT = Record<T>;
					using InputBundleT = RecordBundle<T>;

					/* a space-efficient way to store records that fall in the window.
					 * motivation: in the raw input stream, consecutive records often
					 * fall into the same session window; thus, they can be stored as
					 * (start_record, cnt) instead of individual records.
					 *
					 * the locality exists because we assume that windowing.is done before
					 * GBK (which may break consecutive records into different key groups)
					 */
					struct RecordRange {
						// both inclusive
						RecordT const * start = nullptr;
						RecordT const * end = nullptr;
						//    unsigned long cnt;
					};

					enum class RETCODE {
						OK = 0,
						/* no overlap */
						TOO_EARLY,
						TOO_LATE,
						/* has overlap */
						MODIFY_START,
					};

					// perhaps we should use list since we merge often
					mutable vector<RecordRange> ranges;

					// since we only store pointers (not the actual records in the input bundle),
					// we need to hold pointers to the input bundle so that they do not go away
					// when all the windows referring to an input bundle are purged, the input
					// bundle is automatically destroyed.
					mutable unordered_set<shared_ptr<InputBundleT>> referred_bundles;

					// length of each record
					const boost::posix_time::time_duration record_duration;

					// is the session window *strictly* smaller than the record's window?
					bool operator()(const SessionWindow & lhs, const RecordT & rhs) const {
						return lhs.window_end() <= rhs.ts;
					}

#if 0
					bool operator<(const SessionWindow & lhs, const RecordT & rhs) const {
						return lhs.window_end() <= rhs.ts;
					}
#endif

					bool operator<(const RecordT & rhs) const {
						return this->window_end() <= rhs.ts;
					}

					bool operator<(const SessionWindow& rhs) const {
						return this->start < rhs.start;
					}

					/* see SessionWindowInto::RetrieveWindows() */
					bool operator<(const boost::posix_time::ptime & watermark) const {
						return this->window_end() <= watermark;
					}

#if 1
					/* this may modify window's start point. we cannot call this on any window
					   that is already in a set.
					   @return: whether add succeeds (i.e. record falls into the window).
					 */
					bool try_add_record (RecordT const * rec,
							shared_ptr<InputBundleT> bundle) {
						assert(rec);
						ptime && wstart = window_start();
						ptime && wend = window_end();
						ptime const & rstart = rec->ts;
						ptime rend = rec->ts + record_duration;

						// the window range and record range must intersect
						if (!(
									(rec->ts >= wstart && rec->ts < wend) || (rend >= wstart && rend < wend)
						     ))
							return false;

						// is @rec contiguous wrt to the previous rec that this window receives?
						if (ranges.empty() || ranges.back().end + 1 != rec) {
							ranges.emplace_back();
							ranges.back().start = ranges.back().end = rec;
						} else
							ranges.back().end = rec;

						// update_window
						if (rstart < wstart) { // extend window start
							this->start = rstart - epoch;
							this->duration += (wstart - rstart);
						}

						if (rend > wend) {  // extend window end
							assert(rstart > wstart);
							this->duration += (rend - wend);
						}

						this->referred_bundles.insert(bundle);

						return true;
					}
#endif

					/*  @return true iff
					    1. @rec overlaps with the window,
					    2. the merge does not change the window's starting point
					    since we'll sort windows based on their starting points, this function
					    will not change one window's rank (e.g. in a std::map)

					    otherwise false, nothing will happen
					 */
					RETCODE try_add_record_notbefore (RecordT const * rec,
							shared_ptr<InputBundleT> bundle) const {
						assert(rec);
						ptime && wstart = window_start();
						ptime && wend = window_end();
						ptime const & rstart = rec->ts;
						ptime rend = rec->ts + record_duration;

						/*  [ success ]
						 *
						 *   wstart                       wend
						 *    +----------- window ----------+
						 *            +------------- record ---------+
						 *            rstart                        rend
						 */


						/*
						 *                       wstart                       wend
						 *                        +----------- window ----------+
						 *   +-- record -------+
						 * rstart            rend
						 */
						if (rend <= wstart)
							return RETCODE::TOO_EARLY;

						/*
						 *    wstart                       wend
						 *     +----------- window ----------+
						 *                                            +-- record -------+
						 *                                           rstart            rend
						 */
						if (rstart >= wend)
							return RETCODE::TOO_LATE;

						/*
						 *   [ MODIFY_START, which will modify window's starting point ]
						 *
						 *           wstart                       wend
						 *            +----------- window ----------+
						 *       +------------- record ---------+
						 *      rstart                        rend
						 */
						if (rstart < wstart && rend >= wstart)
							return RETCODE::MODIFY_START;

						// (rend >= wstart && rend < wend)

						// is @rec contiguous wrt to the previous rec that this window receives?
						if (ranges.empty() || ranges.back().end + 1 != rec) {
							ranges.emplace_back();
							ranges.back().start = ranges.back().end = rec;
						} else
							ranges.back().end = rec;

						// update_window
						assert(!(rstart < wstart)); // shouldn't have to extend window start
						if (rend > wend) {  // extend window end
							assert(rstart > wstart);
							this->duration += (rend - wend);
						}

						this->referred_bundles.insert(bundle);

						return RETCODE::OK;
					}

					// merge @other into this window. update this window if succeeds
					// @return: true if merge okay.
					bool merge(SessionWindow const & other) {
						ptime && start = window_start();
						ptime && end = window_end();
						ptime const && ostart = other.window_start();
						ptime const && oend = other.window_end();

						// if no overlap, merge fails
						if (!(
									(ostart >= start && ostart < end) || (oend >= start && oend < end)
						     ))
							return false;

						// update this window
						if (ostart < start) { // extend window start
							this->start = ostart - epoch;
							this->duration += (start - ostart);
						}

						if (oend > end) {  // extend window end
							//      assert(ostart > start);   // possible when we merge window sets
							this->duration += (oend - end);
						}

						this->ranges.insert(this->ranges.end(),
								other.ranges.begin(), other.ranges.end());

						// duplicated ptr to bundles will fail the insertion. so we're fine.
						this->referred_bundles.insert(other.referred_bundles.begin(),
								other.referred_bundles.end());

						return true;
					}

					// the merge succeeds only when it won't modify this window's starting point
					RETCODE merge_notbefore(SessionWindow const &other) const {
						ptime && start = window_start();
						ptime && end = window_end();
						ptime const && ostart = other.window_start();
						ptime const && oend = other.window_end();

						if (oend <= start)
							return RETCODE::TOO_EARLY;

						if (ostart >= end)
							return RETCODE::TOO_LATE;

						if (ostart < start && oend >= start)
							return RETCODE::MODIFY_START;

						if (oend > end) {  // extend window end
							assert(ostart > start); // since we shouldn't extend window start
							this->duration += (oend - end);
						}

						//cout << *this;
						//cout << other;

						this->ranges.insert(this->ranges.end(),
								other.ranges.begin(), other.ranges.end());

						// duplicated ptr to bundles will fail the insertion. so we're fine.
						this->referred_bundles.insert(other.referred_bundles.begin(),
								other.referred_bundles.end());

						return RETCODE::OK;
					}

					void print(ostream & os) const {
						os << "dump SessionWindow: " << to_simplest_string(this->window_start()) \
							<< "--" << to_simplest_string(this->window_end());
						for (auto && range : this->ranges) {
							os << " { ";
							for (auto ptr = range.start; ptr <= range.end; ptr++) {
								if (ptr - range.start <= 4)
									os << (ptr->data) << " ";
								else {
									os << "....";
									break;
								}
							}
							os << " } ";
						}
						os << endl;
					}

					SessionWindow (ptime start, boost::posix_time::time_duration const & duration,
							boost::posix_time::time_duration const & record_duration)
						: Window(start, duration), record_duration(record_duration) { }

				};

			/*
			 * Window comparison functions
			 */

			template<typename T>
				struct WindowRecComp {
					// is the session window *strictly* smaller than the record's window?
					bool operator()(const SessionWindow<T> & lhs, const Record<T> & rhs) const {
						return lhs.window_end() <= rhs.ts;
					}
				};

			template<typename T>
				struct WindowStrictComp {
					// is the 1st session window *strictly* smaller than 2nd window?
					bool operator()(const SessionWindow<T> & lhs,
							const SessionWindow<T> & rhs) const {
						return lhs.window_end() <= rhs.window_start();
					}
				};

			/* compared based on the value of the kvpair. used for sorting
			 * the output kvpairs of a reducer.
			 */
			template<class KVPair>
				struct ReducedKVPairCompLess {
					bool operator()(const KVPair & lhs,
							const KVPair & rhs) const {
						/* since we are going to maintain a min heap */
						return lhs.second > rhs.second;
					}
				};


			template<class KVPair>
				struct ReducedKVPairCompLess1 {
					bool operator()(const KVPair & lhs,
							const KVPair & rhs) const {
						/* since we are going to maintain a min heap */
						return lhs.second < rhs.second;
					}
				};

			////////////////////////////////////////////////////////////////////////

#include <map>
#include <unordered_map>

			/* a bunch of KVS belonging to one window. */
			template <typename K, typename V>
				struct WindowedKVOutput
				{
					typedef pair<K, vector<V>> KVS;

					Window w;
					unordered_map<K, KVS*> vals;

					// return: if key (@k) already exists
					bool add(const K& k, const V& v) {
						bool ret = true;
						if (!vals.count(k)) {
							vals[k] = new KVS();
							ret = false;
						}
						vals[k].second.push_back(v);

						return ret;
					}
				};

#ifndef NDEBUG
#include <atomic>
			static std::atomic<int> cnt = ATOMIC_VAR_INIT(0);
#endif

#include "ValueContainer.h"

#include "WinKeyFrag.h"

#if 0  // obsoleted
			/*
			 * A bundle of multiple WindowFragments (may belong to multi windows).
			 * The output of a GroupBy() from a bundle.
			 */
			template <typename K, typename V>
				struct WindowedKVBundle : public BundleBase {
					using KVS = tuple<K, vector<V>>;

					/* sorted by window start time */
					map<Window, WindowedKVOutput<K,V>*, Window> vals;

					void add(WindowedKVOutput<K,V>* output) {
						vals[output->w] = output;
					}
				};
#endif

			/* A bundle with records in multiple windows.
			 * Each window is assoc with a in Fragments
			 * (i.e. a vector of Records, not grouped by key yet.)
			 *
			 * T: element type, e.g. long
			 *
			 * We do not implement size() since the bundle has a map inside.
			 */
			template <typename T>
				struct WindowsBundle : public BundleBase {

					using RecordT = Record<T>;

					// sorted, by the window time order.
					map<Window, shared_ptr<WindowFragment<T>>, Window> vals;

					void add(shared_ptr<WindowFragment<T>> output) {
						vals[output->w] = output;

						if (output->min_ts < min_ts)
							min_ts = output->min_ts;
					}

					/* can be called with (win, val), for which a record will be auto constructed based on val */
					void add_record(const Window& w, const RecordT& val) {
						add_value(w, val);
					}

					// this is actually "add record"
					// @val must be a timestamped value.
					void add_value(const Window& w, const RecordT& val) {
						if (!vals.count(w)) {
							vals[w] = make_shared<WindowFragment<T>>(w);
							VV("create one window ... %d", vals[w]->id);
						}
						vals[w]->add(val);

						if (val.ts < min_ts)
							min_ts = val.ts;
					}

					// for debugging.
					void show_window_sizes() {
						unsigned long sum = 0;
						for (auto&& it = vals.begin(); it != vals.end(); it++) {
							auto output = it->second;
							assert(output);
							EE("window %d (start %s) size: %lu",
									output->id, to_simple_string(output->w.start).c_str(),
									output->size());
							sum += output->size();
						}
						EE("----- total %lu", sum);
					}

					/* simply ignore the capacity */
					WindowsBundle(int capacity = 0, int node = -1) : BundleBase(node) { };

				};

			/* A bundle with records in multiple windows.
			 *
			 * Each window is assoc with a KeyedFragments (i.e. Records grouped by key)
			 * Windows are sorted (facilitating output them in order).
			 *
			 * XXX merge this with WindowsBundle
			 */
			template <class KVPair,
				 /* WindowKeyFrag can be specialized based on key/val distribution */
				 template<class> class WindowKeyedFragmentT // = WindowKeyedFragment
					 >
					 struct WindowsKeyedBundle : public BundleBase {

						 using K = decltype(KVPair::first);
						 using V = decltype(KVPair::second);
						 using RecordKV = Record<KVPair>;
						 //  using WindowFragmentKV = WindowKeyedFragment<KVPair>;

						 /* no need to use concurrent map */
						 //  using WindowFragmentKV = WindowKeyedFragment<KVPair,
						 //  		std::unordered_map<decltype(KVPair::first),
						 //  												ValueContainer<decltype(KVPair::second)>>>;
						 using WindowFragmentKV = WindowKeyedFragmentT<KVPair>;

						 // ordered, can iterate in window time order.
						 // embedding WindowFragmentKV, which does not contain the actual Vs
						 map<Window, WindowFragmentKV, Window> vals;

						 // copy a given kvpair record under the corresponding window
						 // and key.
						 // XXX have a lockfree implementation
						 void add_record(const Window& w, const RecordKV& val) {
							 //    if (!vals.count(w)) {
							 //        VV("create one window ... %d", vals[w].id);
							 //    }
							 //    assert(val.data.second != 0);
							 vals[w].add_record_unsafe(val); /* since this works on a bundle */

							 if (val.ts < min_ts)
								 min_ts = val.ts;
						 }

						 /* ignore the capacity */
						 WindowsKeyedBundle(int capacity = 0, int node = -1) : BundleBase(node) { };
					 };

			/* not useful. use WindowsBundle
			 * similar to above, but each k has one v.
			 */
#if 0
			template <class KVPair>
				struct WindowsKVBundle : public BundleBase {
					using K = decltype(KVPair::first);
					using V = decltype(KVPair::second);
					using RecordKV = Record<KVPair>;
					using WindowFragmentKV = WindowKeyedFragment<KVPair, SingleValueContainer<V>>;

					// ordered, can iterate in window time order.
					// embedding WindowFragmentKV, which does not contain the actual Vs
					map<Window, WindowFragmentKV, Window> vals;

					// copy a given kvpair record under the corresponding window
					// and key.
					// XXX have a lockfree implementation
					void add_record(const Window& w, const RecordKV& val) {
						vals[w].add_record_unsafe(val); /* since this works on a bundle */
						if (val.ts < min_ts)
							min_ts = val.ts;
					}

					/* ignore the capacity */
					WindowsKVBundle(int capacity = 0, int node = -1) : BundleBase(node) { };
				};
#endif

			/* iterator that goes through each <key, value_container> pair in a window
			 * map.
			 *
			 * A window map often has multiple windows, each of which associated with
			 * multiple <key, value_container> pairs.
			 *
			 * an evaluator's unit of work is on values of same window/key,
			 * and it may need to distribute multi units of work among workers.
			 * this iterator interface makes such distribution easier.
			 */
			template<typename KVPair>
				class window_map_iterator {

					using WindowMap = map<Window, WinKeyFrag_Std<KVPair>, Window>;
					using K = decltype(KVPair::first);
					using V = decltype(KVPair::second);
					using ValueContainerT = ValueContainer<V>;
					/* keys and their value containers (for a given window) */
					using KeyMap = typename WinKeyFrag_Std<KVPair>::KeyMap;
					//	using KeyMap = tbb::concurrent_unordered_map<K, ValueContainerT>;

					private:
					WindowMap const * _windows;
					typename WindowMap::const_iterator it_win;
					typename KeyMap::const_iterator it_kvcontainer;

					/* Start from the current iter position, skip any empty windows. Stop at:
					 * 1. the 1st valid item (e.g. begin() of the 1st non-empty win)
					 * 2. end() of the last win (which must be empty).
					 *
					 * If already points to a valid item, do nothing and stop there.
					 */
					void skip_empty_windows() {

						assert(!_windows->empty());

						auto lastbutone = _windows->end();
						std::advance(lastbutone, -1);  // last window.

						// currently pointing to a valid item
						while ( (it_kvcontainer == it_win->second.vals.end()) ) {
							// pointing to a window's end()
							if (it_win == lastbutone) {
								/* if we are at the last window, stop */
								return;
							} else {
								// moved to the beginning of the next window
								it_win++;
								it_kvcontainer = it_win->second.vals.begin();
								VV("moved to next win: %s",
										to_simplest_string(it_win->first.window_start()).c_str());
							}
						}
					}

					public:
					window_map_iterator(WindowMap const * windows,
							typename WindowMap::const_iterator it_win,
							typename KeyMap::const_iterator it_kvcontainer) :
						_windows(windows), it_win(it_win), it_kvcontainer(it_kvcontainer)
					{
						skip_empty_windows(); /* needed as the 1st win may be empty */
					}

					bool operator!=(window_map_iterator const& other) const {
						/* cannot swap the order: debugging stl will complain */
						return (it_win != other.it_win || it_kvcontainer != other.it_kvcontainer);
					}

					window_map_iterator& operator++() {

#if 0
						/* undefined: if we already hit the end of the last window */
						assert (!(it_kvcontainer == it_win->second.vals.end()
									&& it_win == windows->end()));
#endif

						/* capture the following two situations:
						 * 1. undefined: if we already hit the end of the last window and still try
						 * to ++;
						 * 2. bug: we somehow stopped at the end of an intermediate window
						 */
						if (it_kvcontainer == it_win->second.vals.end()) {
							EE("bug: distance to win end %lu", std::distance(it_win, _windows->end()));
						}

						assert(it_kvcontainer != it_win->second.vals.end());

						it_kvcontainer++;

						skip_empty_windows();

						return *this;
					}

					/* xzl: why we get a warning here if we return by reference? */
					pair<K, ValueContainerT> const operator*() const {
						/* the internal iterator should be valid */
						assert(it_kvcontainer != it_win->second.vals.end());
#if 0
						if (it_kvcontainer == it_win->second.vals.end()) {
							auto lastbutone = _windows->end();
							lastbutone --;

							window_map_iterator itend(_windows, lastbutone,
									lastbutone->second.vals.end());

							if (it_win == lastbutone) {
								W("we are last but one");

								if (*this != itend) {
									W("we dont equal to end");
								} else
									W("we equal to end");
							} else
								W("we are NOT last but one");

							assert(0);
						}
#endif

						return *it_kvcontainer;
					}

					Window const & get_current_window() const {
						return it_win->first;
					}

				};

			/////////////////////////////////////////////////////////

			/* similar to above, but @map uses shared_ptr<WindowKeyedFragmentStd> */

			template<typename KVPair>
				class window_map_iterator2 {

					using WindowMap = map<Window, shared_ptr<WinKeyFrag_Std<KVPair>>, Window>;

					using K = decltype(KVPair::first);
					using V = decltype(KVPair::second);
					using ValueContainerT = ValueContainer<V>;
					/* keys and their value containers (for a given window) */
					using KeyMap = typename WinKeyFrag_Std<KVPair>::KeyMap;

					private:
					WindowMap const * _windows;
					typename WindowMap::const_iterator it_win;
					typename KeyMap::const_iterator it_kvcontainer;

					/* Start from the current iter position, skip any empty windows. Stop at:
					 * 1. the 1st valid item (e.g. begin() of the 1st non-empty win)
					 * 2. end() of the last win (which must be empty).
					 *
					 * If already points to a valid item, do nothing and stop there.
					 */
					void skip_empty_windows() {

						xzl_assert(!_windows->empty());

						auto lastbutone = _windows->end();
						std::advance(lastbutone, -1);  // last window.

						// currently pointing to a valid item
						while ( (it_kvcontainer == it_win->second->vals.end()) ) {
							// pointing to a window's end()
							if (it_win == lastbutone) {
								/* if we are at the last window, stop */
								return;
							} else {
								// moved to the beginning of the next window
								it_win++;
								it_kvcontainer = it_win->second->vals.begin();
								VV("moved to next win: %s",
										to_simplest_string(it_win->first.window_start()).c_str());
							}
						}
					}

					public:
					window_map_iterator2(WindowMap const * windows,
							typename WindowMap::const_iterator it_win,
							typename KeyMap::const_iterator it_kvcontainer) :
						_windows(windows), it_win(it_win), it_kvcontainer(it_kvcontainer)
					{
						skip_empty_windows(); /* needed as the 1st win may be empty */
					}

					bool operator!=(window_map_iterator2 const& other) const {
						/* cannot swap the order: debugging stl will complain */
						return (it_win != other.it_win || it_kvcontainer != other.it_kvcontainer);
					}

					/* this can be hot: reducer will invoke this to partition all keys */
					window_map_iterator2 & operator++() {

						/* capture the following two situations:
						 * 1. undefined: if we already hit the end of the last window and still try
						 * to ++;
						 * 2. bug: we somehow stopped at the end of an intermediate window
						 */
						if (it_kvcontainer == it_win->second->vals.end()) {
							EE("bug: distance to win end %lu", std::distance(it_win, _windows->end()));
						}

						assert(it_kvcontainer != it_win->second->vals.end());

						it_kvcontainer++;

						skip_empty_windows();

						return *this;
					}

					/* xzl: why we get a warning here if we return by reference? */
					pair<K, ValueContainerT> const operator*() const {
						/* the internal iterator should be valid */
						assert(it_kvcontainer != it_win->second->vals.end());
						return *it_kvcontainer;
					}

					Window const & get_current_window() const {
						return it_win->first;
					}

				};

			/////////////////////////////////////////////////////////

			/* pointing to a word in the input buffer.
			 *
			 * this source transform will not create copies of the data, but will defer it
			 * to downstream transforms (e.g. mapper)
			 */
			struct word {
				const char *data;
				uint32_t len;
			};

			/* represented a segment in the input buffer */
			struct string_range {
				const char *data;
				uint64_t len;
			};

			//hym: for Network latency app
			struct srcdst_rtt{
				char sd_ip[32];
				long rtt;
			};

			struct Event {
				//long timeStamp;
				//boost::posix_time::ptime timestamp;
				//boost::uuids::uuid user_id;
				//boost::uuids::uuid page_id;
				//boost::uuids::uuid ad_id;
				creek::string user_id;
				creek::string page_id;
				creek::string ad_id;
				creek::string ad_type;
				//std::string event_type;
				int num_event_type;
				//boost::asio::ip::address ip;
				int ip;
			};
			/////////////////////////////////////////////////////////

			/* an array of set for fixing the scalability bottleneck in a single set.
			 *
			 * for distinct count */
			namespace creek_set_array {
				/* good: 16 seems best
				 * 32 -- worse
				 * 128 -- bad
				 */
#define NUM_SETS 16
				//	using SetArray = tbb::concurrent_unordered_set<creek::string>[NUM_SETS];
				using SetArray = std::array<tbb::concurrent_unordered_set<creek::string>, NUM_SETS>;
				using SetArrayPtr = shared_ptr<SetArray>;

				void insert(SetArrayPtr ar, creek::string const & in);

				/* merge two sets */
				void merge(SetArrayPtr mine, SetArrayPtr const & others);

			}

			/////////////////////////////////////////////////////////

			//#include "creek-map.h"

#endif // TRANSFORMS_H

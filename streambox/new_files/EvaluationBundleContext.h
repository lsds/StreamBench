/*
 * Common state for the whole pipeline. Mostly used to maintain the
 * pipeline topology. This is the "bundle" version.
 * XXX change to a better name
 *
 * Author: xzl
 *
 * Purdue University, 2016
 */

/*********************************************************
*
*  Copyright (C) 2014 by Vitaliy Vitsentiy
*
*  Licensed under the Apache License, Version 2.0 (the "License");
*  you may not use this file except in compliance with the License.
*  You may obtain a copy of the License at
*
*     http://www.apache.org/licenses/LICENSE-2.0
*
*  Unless required by applicable law or agreed to in writing, software
*  distributed under the License is distributed on an "AS IS" BASIS,
*  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
*  See the License for the specific language governing permissions and
*  limitations under the License.
*
*********************************************************/

#ifndef EVAL_BUNDLE_CONTEXT_H
#define EVAL_BUNDLE_CONTEXT_H

#include <pthread.h>
#include <unordered_set>
#include <condition_variable>

#include "boost/date_time/posix_time/posix_time_types.hpp"

#include "config.h"
#include "Pipeline.h"
#include "LookAheadPipelineVisitor.h"
#include "Transforms.h"
#include "Values.h"
//#include "Executor.h"

#include "log.h"

//#ifndef USE_NUMA_TP
//#include "ctpl.h"
#include "ctpl_stl.h"
//#endif

#include "Join/Join.h"

#ifdef USE_NUMA_TP
#include "utilities/threading.hh"
using namespace Kaskade;
#endif

using namespace std;
using namespace boost::date_time;
using namespace boost::posix_time;

// Shared among concurrent executions of evaluators.
class EvaluationBundleContext {

	// ------------------------------------------------------------- //

	/* The customized, thread pool based, execution engine for streaming pipeline.
 	 * We use pthread in case we may upgrade to NUMA-aware tp later.
 	 * Part of code comes from ctpl. See below. */

	class Executor {

		template <typename T>
		class TaskQueue {
		public:
				bool push(T const & value) {
						std::unique_lock<std::mutex> lock(this->mutex);
						this->q.push(value);
						return true;
				}
				// deletes the retrieved element, do not use for non integral types
				bool pop(T & v) {
						std::unique_lock<std::mutex> lock(this->mutex);
						if (this->q.empty())
								return false;
						v = this->q.front();
						this->q.pop();
						return true;
				}
				bool empty() {
						std::unique_lock<std::mutex> lock(this->mutex);
						return this->q.empty();
				}

				// xzl: debugging
				int size() {
					std::unique_lock<std::mutex> lock(this->mutex);
					return this->q.size();
				}

		private:
				std::queue<T> q;
				std::mutex mutex;
	};

	private:
		struct thread_param {
			int id;
			EvaluationBundleContext *c;
			Executor *executor;
		};

	public:
		Executor (EvaluationBundleContext* c, int nthreads)
			: c_(c), threads_(nthreads), nwait_(0) {
			assert(nthreads > 0 && nthreads < 200);
		}

		~Executor() {
			this->StopThreads();
		}

		/* fire up all threads */
		bool StartThreads() {

			for (unsigned int i = 0; i < threads_.size(); i++) {
				/* to be free'd by the thread */
				thread_param *tp = (thread_param *)malloc(sizeof(thread_param));
				assert(tp);
				tp->id = i;
				tp->executor = this;
				tp->c = c_;

				int rc = pthread_create(&threads_[i], NULL, run, tp);
				xzl_assert(rc == 0);
			}
			EE("%lu threads fired up...", threads_.size());

			return true;
		}

		void StopThreads() {
			if (isStop)
				return;

			isStop = true;
			{
				std::unique_lock<mutex> lck(cv_mtx_);
				cv_.notify_all();
			}

			EE("--- start to stop worker threads...");
			for (auto & t : threads_) {
				pthread_join(t, NULL);
			}
			EE("--- all worker threads stopped.");

			threads_.clear();
		}

		/* after producing work, call this to notify any waiting threads */
		void Notify() {
			VV("----- notify ------ ");
			std::unique_lock<mutex> lck(cv_mtx_);
			/* XXX increase a global work counter? XXX */
			cv_.notify_one();
		}

		/* ----------
		 * task queue
		 * ---------- */

	public:
	  // pops a functional wrapper to the original function
	  std::function<void(int)> pop() {
	      std::function<void(int id)> * _f = nullptr;
	      this->q.pop(_f);
	      std::unique_ptr<std::function<void(int id)>> func(_f); // at return, delete the function even if an exception occurred
	      std::function<void(int)> f;
	      if (_f)
	          f = *_f;
	      return f;
	  }

	  template<typename F, typename... Rest>
	  auto push(F && f, Rest&&... rest) ->std::future<decltype(f(0, rest...))> {
	      auto pck = std::make_shared<std::packaged_task<decltype(f(0, rest...))(int)>>(
	          std::bind(std::forward<F>(f), std::placeholders::_1, std::forward<Rest>(rest)...)
	          );
	      auto _f = new std::function<void(int id)>([pck](int id) {
	          (*pck)(id);
	      });
	      this->q.push(_f);
//	      std::unique_lock<std::mutex> lock(this->mutex);
//	      this->cv.notify_one();
	      Notify();
	      return pck->get_future();
	  }

	  // run the user's function that excepts argument int - id of the running thread. returned value is templatized
	  // operator returns std::future, where the user can get the result and rethrow the catched exceptins
	  template<typename F>
	  auto push(F && f) ->std::future<decltype(f(0))> {
	      auto pck = std::make_shared<std::packaged_task<decltype(f(0))(int)>>(std::forward<F>(f));
	      auto _f = new std::function<void(int id)>([pck](int id) {
	          (*pck)(id);
	      });
	      this->q.push(_f);
//	      std::unique_lock<std::mutex> lock(this->mutex);
//	      this->cv.notify_one();
	      Notify();
	      return pck->get_future();
	  }

	private:
		std::condition_variable cv_;
		std::mutex cv_mtx_; 				/* only for protecting the cv */
		bool isStop = false;
	  TaskQueue<std::function<void(int id)> *> q;  /* task queue */

		/* -- main func for a worker thread -- */
		static void * run(void *t) {

			assert(t);
			thread_param *tp = (thread_param *)t;
			EvaluationBundleContext *c = tp->c;
			Executor *exec = tp->executor;
			int id = tp->id;
			assert(exec);

			free(tp);

			shared_ptr<BundleBase> bundleptr;
			PTransform *trans;

			std::function<void(int id)> * _f;
			bool isPop;
			bool has_work;

			cpu_set_t cpuset;
			CPU_ZERO(&cpuset);
			int cpu_core = (2 * id) %20;
			CPU_SET(cpu_core + 1, &cpuset);
			pthread_t current_thread = pthread_self();
			pthread_setaffinity_np(current_thread, sizeof(cpu_set_t), &cpuset);
            std::cout << "Worker Thread #" << id << ": on CPU " << sched_getcpu() << "\n";


			while (true) {
				/* XXX check a global work counter? XXX */

				while (true) {

					has_work = false;

					/* drain the task queue */
					while ( (isPop = exec->q.pop(_f)) ) {  // if there is anything in the queue
	//				if ( (isPop = exec->q.pop(_f)) ) {  // if there is anything in the queue
							std::unique_ptr<std::function<void(int id)>> func(_f); // at return, delete the function even if an exception occurred
							try {
								(*_f)(id);
							} catch (const std::exception& e) {
								std::cout << " a standard exception was caught, with message '"
													<< e.what() << "'\n";
								abort();
							}
							has_work = true;
					}

					//std::cout << "Thread #" << id << ": on CPU " << sched_getcpu() << "\n";

					/* no task right now, consume one bundle/punc before checking tasks
					 * again. */
	//				while ((bundleptr = c->getOneBundle(&trans))) {
					if ((bundleptr = c->getOneBundle(&trans))) {
						assert(trans);
						VV("th %d: got a bundle for %s", id, trans->name.c_str());

						try {
							trans->ExecEvaluator(0, /* nodeid, XXX */ c, bundleptr);
						} catch (const std::exception& e) {
							std::cout << " a standard exception was caught, with message '"
												<< e.what() << "'\n";
							abort();
						}
						has_work = true;
					}

					/* this iteration got nothing from task and containers. sleep. */
					if (!has_work)
						break;
				}

				if (exec->isStop)
					break;

				/* no work to do. wait to be signaled */
				VV("th %d: no work to do. wait...", id);
//				EE("th %d: no work to do. wait...", id);
				{
                    //printf("th %d: gamietai h mana tou. wait... \n", id);
					std::unique_lock<mutex> lck(exec->cv_mtx_);
					exec->nwait_ ++;
//					cv_.wait(lck, [] {
//							/* XXX check a global work counter? check isStop?
//							 * do we really have work to do? XXX */
//							return true;
//						});
					exec->cv_.wait(lck);
					/* woken up and have the lock */
					exec->nwait_ --;
				}
				/* unlocked */

				if (exec->isStop)
					break;
			}

			return NULL;
		}

		EvaluationBundleContext *c_;
		vector<pthread_t> threads_;
	public:
		atomic<long> nwait_;
	};

	// ------------------------------------------------------------- //

  // http://stackoverflow.com/questions/30425772/c-11-calling-a-c-function-periodically?answertab=votes#tab-top
  /* This launches a dedicated thread that executes the given func
   * periodically.
   */
  class CallBackTimer
  {
  public:
      CallBackTimer()
      :_execute(false)
      {}

      ~CallBackTimer() {
          if( _execute.load(std::memory_order_acquire) ) {
              stop();
          };
      }

      void stop()
      {
          _execute.store(false, std::memory_order_release);
          if( _thd.joinable() )
              _thd.join();
      }

      void start(int interval, std::function<void(void)> func)
      {
          if( _execute.load(std::memory_order_acquire) ) {
              stop();
          };
          _execute.store(true, std::memory_order_release);
          _thd = std::thread([this, interval, func]()
          {
              while (_execute.load(std::memory_order_acquire)) {
                  func();
                  std::this_thread::sleep_for(
                  std::chrono::milliseconds(interval));
              }
          });
      }

      bool is_running() const noexcept {
          return ( _execute.load(std::memory_order_acquire) &&
                   _thd.joinable() );
      }

  private:
      std::atomic<bool> _execute;
      std::thread _thd;
  };

	// ------------------------------------------------------------- //

public:
  LookAheadPipelineVisitor _visitor; // require full decl of the visitor

  // simply saved all values. we knew @Pipeline has _values but we don't
  // pass @Pipeline around...
  unordered_set<PValue *> _values;

//  unordered_set<PTransform *> _transforms;
  vector<PTransform *> _transforms; // (partially) ordered. upstream comes first

  atomic<long> spawn_cnt_[4]; /* # of spawn tasks on each node. debugging */

  // ctor. must specify target_wm_delta
  EvaluationBundleContext(int target_wm_delta_secs,
  		/* save 1 thread for the source */
  		unsigned int num_workers = std::thread::hardware_concurrency() - 1)
    : _visitor(this),
      tp(0),
      num_workers(num_workers),
      executor_(this, num_workers),
      target_wm_delta_(seconds(target_wm_delta_secs))
  {
    I("hw concurrency = %u, num_workers = %d",
    		std::thread::hardware_concurrency(),
    		num_workers);

    xzl_bug_on_msg(num_workers > std::thread::hardware_concurrency() - 1,
    		"too many workers");

    xzl_bug_on_msg(num_workers == 0, "too few workers");

    for (auto & cnt : spawn_cnt_) {
    	cnt = 0;
    }

    num_bundles_beforewm = 0;
    num_bundles_afterwm = 0;

  }

private:
  /* we dump the topology, and add all pvalues to a set; all ptransforms
   * to a set as well. */
  void traverseTopology(PValue *b) {
    PValue *v = b;
    PTransform *t = nullptr;

    while (true) {

        _values.insert(v);

        printf("%s --> ", v->getName().c_str());

        t = v->consumer;
        if (!t)
          break;

//        _transforms.insert(t);
        _transforms.push_back(t);

        printf("%s --> ", t->getName().c_str());
        if (t->outputs.size() == 0)
          break;

        if (t->outputs.size() == 2) {
            traverseTopology(t->outputs[1]);
          printf("\r");
        }

        v = t->outputs[0];
    }
    printf("\n");
  }

public:
  void run (Pipeline* p) {
    // kick start eval of the source transform
    assert(begin);
    RunConsumer(begin, -1);
  }

  void runSimple (Pipeline* p) {
    this->begin = p->saved_begin;
    this->source = p->saved_begin->consumer;

    assert(begin);

    /* traverse topology (and print it out for debugging)
     *
     * record all values so that the source can easily measure their
     * pressure.
     * NB: we cannot use traverseTopologically() since we ditch all this
     * hierarchy API.
     */
    assert(_values.size() == 0);
    traverseTopology(begin);

#if 0
    PValue *v = begin;
    PTransform *t = nullptr;
    while (true) {
        printf("%s --> ", v->getName().c_str());

        t = v->consumer;
        if (!t)
          break;

        printf("%s --> ", t->getName().c_str());
        if (t->outputs.size() == 0)
          break;

        v = t->outputs[0];
    }
    printf("\n");
#endif
    executor_.StartThreads();

    stat_collector_.start(3000 /* internal ms */,
    		std::bind(&EvaluationBundleContext::getStatistics, this));

    RunConsumer(begin, -1);
//    sleep(3);

    stat_collector_.stop();
    executor_.StopThreads();
  }

  // directly exec the downstream transform in the current thread (i.e sync)
  void RunConsumer (PValue *v, int nodeid);

  // the thread pool version
  void SpawnConsumer (PValue *v, int node = -1);
  // in-house tp. only notify the threads. threads will find work
  void SpawnConsumer() {
  	executor_.Notify();
  }

  PTransform * source = nullptr;  // we assume there's only one source. XXX
  PValue * begin = nullptr; // the dummy, "begin" value that precedes the source

  /* @wm: the snapshot of an upstream watermark */
//  void OnNewUpstreamWatermark(ptime wm, PTransform *trans);

	/* update the watermark of @start (not necessarily the source),
	 and propagate changes downstream.
	 This function should be sync, i.e. by the time it returns, the watermark
	 reaches the entire downstream. Each transform may spawn a bunch of workers,
	 e.g. in purging the state, but the caller should join() those workers.
	 */

  void PropogateWatermark(PTransform *start) {
  	assert (0);
#if 0 /* no longer in use since we propagate "snapshots" of wm */
    // a standard BFS to traverse the graph of transforms
    deque<PTransform*> l;
    unordered_set<PTransform*> visited;

    l.push_back(start);
    while (l.size()) {
        PTransform * p = l.front(); l.pop_front();

        I("visited %s:", p->name.c_str());
        // first let the transform to maintain/update its watermark
        ptime wm = p->RefreshWatermark();
        // then dispatch to the transform eval's callback
        RefreshTransformWatermark(wm, p);
        visited.insert(p);

        // add all p's immediate downstream transforms to the working queue
        for (auto && it = p->outputs.begin(); it != p->outputs.end(); it++) {
          auto & child = (*it)->consumer;
          if (child) {
            // only add this transform if all its immediate upstream transforms
            // are visited. otherwise, skip this transform since we know it will
            // be add to the queue by an upstream transform later.
            for (auto && itt = child->inputs.begin(); itt != child->inputs.end(); itt++) {
              if ((*itt)->producer && visited.count((*itt)->producer) == 0) {
                  // this child has an unvisited parent
                  goto next_child;
              }
            }
            l.push_back(child);
          }
          next_child:
            ;
        }
    }
#endif
  }

  /* propagate a snapshot of @start's wm (not @start's current wm). */
  void PropogateWatermarkSnapshot(PTransform *start, ptime const start_wm) {
#if 0
    for (PTransform* p = start; p; p = p->getFirstOutput()->consumer) {
        // first let the transform to maintain/update its watermark
        ptime wm = p->RefreshWatermark();
        // then dispatch to the transform eval's callback
        OnTransformWatermarkRefresh(wm, p);
    }
#endif

    /* a standard BFS to traverse the graph of transforms (in topological order) */

    /* each element: i) a transform and ii) the snapshot of its upstreaming's wm */
    deque<pair<PTransform*,ptime>> l;
    unordered_set<PTransform*> visited;

#if 0
//    PTransform * p = start;
//    ptime wm = start_wm;

//    RefreshTransformWatermark(wm, p);
    // add all start's immediate downstream transforms to the working queue
    for (auto && it = p->outputs.begin(); it != p->outputs.end(); it++) {
      auto & child = (*it)->consumer;
      if (child) {
        // only add this transform if all its immediate upstream transforms
        // are visited. otherwise, skip this transform since we know it will
        // be add to the queue by an upstream transform later.
        for (auto && itt = child->inputs.begin(); itt != child->inputs.end(); itt++) {
          if ((*itt)->producer && visited.count((*itt)->producer) == 0) {
              // this child has an unvisited parent
              goto next_child1;
          }
        }
        l.push_back(make_pair(child, wm));
      }
      next_child1:
        ;
    }
#endif

    l.push_back(make_pair(start, start_wm));

    while (l.size()) {
    	auto pair = l.front(); l.pop_front();
      PTransform * p = pair.first;
      ptime wm = pair.second;  /* upstream's wm snapshot */
      ptime this_wm;

			I("visited %s:", p->name.c_str());

			if (p != start) {
        // 1. make the transform to recalculate its watermark
//				ptime old_wm = p->watermark; /* peek */
				ptime old_wm = p->GetWatermarkSafe(); /* peek */
				this_wm = p->RefreshWatermark(wm);
				if (old_wm == this_wm) {
					/* do nothing to current transform, and skip all downstream */
					visited.insert(p);
					continue;
				}
        // 2. let the transform react to the new upstream wm
				// XXX this should block until all state prior to @this_wm reaches downstream
				// during the course, @p's wm may have advanced; but we're good --
				// we will propagate a snapshot @wm
//				OnNewUpstreamWatermark(wm, p);
			} else
				this_wm = wm;

			visited.insert(p);

			// add all p's immediate downstream transforms to the working queue
			for (auto && it = p->outputs.begin(); it != p->outputs.end(); it++) {
				auto & child = (*it)->consumer;
				if (child) {
					// only add this transform if all its immediate upstream transforms
					// are visited. otherwise, skip this transform since we know it will
					// be add to the queue by an upstream transform later.
					for (auto && itt = child->inputs.begin(); itt != child->inputs.end(); itt++) {
						if ((*itt)->producer && visited.count((*itt)->producer) == 0) {
								// this child has an unvisited parent
								goto next_child;
						}
					}
					l.push_back(make_pair(child, this_wm));
				}
				next_child:
					;
			}
    }
  }

  // the source transform injects a new watermark to the pipeline.
  // only applies on the source. XXX support multiple sources.
  void SeedWatermark(ptime watermark) {
    assert(source);
//    assert(source->watermark < watermark); // watermark increases monotonically
//    source->watermark = watermark;
    source->SetWatermarkSafe(watermark);
    PropogateWatermark(source);
  }

  /* split the above func into two, so that we can wait for all outstanding
   * bundles in between.
   */
  void UpdateSourceWatermark(ptime watermark) {
    assert(source);
    if (source->GetWatermarkSafe() > watermark) {
    	EE("error: source watermark %s > new watermark %s",
    			to_simple_string(source->GetWatermarkSafe()).c_str(),
    			to_simple_string(watermark).c_str());
    } else {
#if 0  /* debugging */
    	EE("%s: wm: %s...",
					to_simple_string(boost::posix_time::microsec_clock::local_time()).c_str(),
					to_simple_string(watermark).c_str());
#endif
    }

//    assert(source->GetWatermark() <= watermark); // watermark increases monotonically
//    source->watermark = watermark;
    source->SetWatermarkSafe(watermark);
  }

  void PropagateSourceWatermark() {
  	PropogateWatermark(source);
  }

  void PropagateSourceWatermarkSnapshot(const ptime wm) {
  	PropogateWatermarkSnapshot(source, wm);
  }

  ctpl::thread_pool tp;  /* even for NUMA, keep this tp at our disposal */
  /* XXX get rid of this. no longer in use */

  const unsigned int num_workers; /* # of total worker threads in executor */

#ifdef USE_NUMA_TP
  NumaThreadPool& pool = NumaThreadPool::instance();
#endif

  Executor executor_;
  CallBackTimer stat_collector_;

  /*
   * for pipeline-wide job stealing
   */

  /* the next wm the pipeline should work towards.
   *
   * -- pipeline sets @target_wm to an estimated value, e.g. based on the next
   * window boundary.
   *
   * -- once the source sees the first punc that exceeds the estimated @target_wm
   * (>= @target_wm), it advances @target_wm to be the punc.
   *
   * -- finally, when the sink receives @target_wm, @target_wm is advanced and estimated
   *
   * Also vary bundle sizes depending on their distance to next wm.
   *
   * XXX can we update wm atomically to avoid lock? XXX
   */

  boost::shared_mutex target_wm_mutex_;
  ptime target_wm_ = max_date_time; /* initially, we'll consume any bundles */
  boost::posix_time::time_duration target_wm_delta_;

public:

  /* for statistics */
  atomic<long> num_bundles_beforewm;
  atomic<long> num_bundles_afterwm;

  ptime AdvanceTargetWm(boost::posix_time::time_duration const & target_wm_delta) {
  	boost::unique_lock<boost::shared_mutex> wlock(target_wm_mutex_);
  	target_wm_ += target_wm_delta;
  	return target_wm_;
  }

  ptime AdvanceTargetWm() {
  	boost::unique_lock<boost::shared_mutex> wlock(target_wm_mutex_);
  	target_wm_ += target_wm_delta_;
  	VV("target_wm set to %s", to_simplest_string(target_wm_).c_str());
  	return target_wm_;
  }

  ptime GetTargetWm() {
  	boost::shared_lock<boost::shared_mutex> rlock(target_wm_mutex_);
  	return target_wm_;
  }

  void SetTargetWm(ptime const & t) {
//  	assert(t > target_wm_ && "must advance monotonically");
  	boost::unique_lock<boost::shared_mutex> wlock(target_wm_mutex_);
  	target_wm_ = t;
  	EE("----> target_wm set to %s", to_simple_string(target_wm_).c_str());
  	//VV("----> target_wm set to %s", to_simple_string(target_wm_).c_str());
  }

  /* sink call this to notify the eval context that it sees a punc.
   * accordingly, the eval context will adjust its target wm.
   */
  void OnSinkGetWm(ptime const & sink_wm) {
  	ptime newwm = sink_wm + target_wm_delta_;
  	SetTargetWm(newwm);
//  	AdvanceTargetWm();
  }

  /* call into every transform to see if they have something to report.
   * to be called periodically.
   */
  void getStatistics() {
  	PTransform::Statstics s;

  	printf("----- Pipeline Statistics  ---- \n");
  	printf("%25s%10s%10s%10s%10s\n", "Trans", "MB/s", "Avg",
  			"KRec/s", "Avg");

    for (auto && t : this->_transforms) {
    	if (t->ReportStatistics(&s))
    		printf("%25s%10.2f%10.2f%10.2f%10.2f\n", s.name,
    				s.lmbps / (1024 * 1024), s.mbps / (1024 * 1024),
    				s.lmrps / (1024), s.mrps / 1024);
    }
    /* */
		printf("processed num_bundles_beforewm %ld num_bundles_afterwm %ld\n",
			 this->num_bundles_beforewm.load(std::memory_order_relaxed),
			 this->num_bundles_afterwm.load(std::memory_order_relaxed));
		printf("thr idle %ld\n", this->executor_.nwait_.load());
		printf("-------------------------------- \n");

		// dbg
#if 0
  	for (auto && t : this->_transforms) {
//  		cout << t->name <<  ":" << t->getNumBundles() << " ";
//  		t->dump_containers();
  		cout << t->getContainerInfo();
  	}
#endif
  }

  /* The main scheduler.
   *
   * Retrieve one bundle from any transform in the pipeline.
   * Worker threads keep calling this to work on bundles.
   *
   * In job stealing, give preference to:
   * 1. bundles whose ts < @target_wm.
   * 2. upstream bundles
   * 3. downstream bundles whose ts < @target_wm (like in open containers?)
   *
   * it appears sophisticated strategy, e.g. going through container lists mutliple
   * times, each with a incremented taget_wm, does not lead to lower latency.
   *
   * obsoleted:
   * If an upstream has an unemitted punc < @target_wm, we should consider
   * the open containers (i.e. punc unassigned) in the downstream.
   *
   * @return: the found bundle
   * @ptrans: [OUT] the transform for which eval should be executed.
   *
   * The caller holds no lock.
   */
  shared_ptr<BundleBase> getOneBundle(PTransform ** ptrans, int node = -1) {

  	bool has_pending_punc = false;

  	/* Steal jobs based on the targeted wm */
//  	for (auto w = target_wm_; w <= target_wm_ + seconds(6); w+= seconds(2)) {
		for (auto & t : this->_transforms) {
/*				auto bundleptr = t->getOneBundleOlderThan(target_wm_, &has_pending_punc, node);
				if (bundleptr) {
					*ptrans = t;
					num_bundles_beforewm.fetch_add(1, memory_order_relaxed);
					return bundleptr;
				}
*/
			// type 5: join's downstream's downstream
			if (t->get_side_info() == SIDE_INFO_JDD) {
				auto bundleptr = t->getOneBundleOlderThan_5(target_wm_, &has_pending_punc, node);
				if(bundleptr){
					 *ptrans = t;
					 num_bundles_beforewm.fetch_add(1, memory_order_relaxed);
					 return bundleptr;
				}
  			// type 4 trans:  Join's downstream
			} else if(t->get_side_info() == SIDE_INFO_JD) {
				auto bundleptr = t->getOneBundleOlderThan_4(target_wm_, &has_pending_punc, node);
				if(bundleptr){
					 *ptrans = t;
					 num_bundles_beforewm.fetch_add(1, memory_order_relaxed);
					 return bundleptr;
				}
			// type 3 trans: Join
			} else if(t->get_side_info() == SIDE_INFO_J) {
				/* XXX xzl: this is REALLY nasty */
				shared_ptr<BundleBase> bundleptr;
#if 0
			 Join<pair<long, long>>* jt = dynamic_cast<Join<pair<long, long>> *>(t);
				if (jt) {
					 bundleptr = jt->getOneBundleOlderThan(target_wm_, &has_pending_punc, node);
				} else {
					Join<pair<long, long>, RecordBundle, RecordBundle>* jt1
						= dynamic_cast<Join<pair<long, long>, RecordBundle, RecordBundle> *>(t);
					xzl_bug_on(!jt1);  /* what types can Join be? */
					bundleptr = jt1->getOneBundleOlderThan(target_wm_, &has_pending_punc, node);
				}
#endif
				bundleptr = t->getOneBundleOlderThan(target_wm_, &has_pending_punc, node);
				if (bundleptr) {
					*ptrans = t;
					num_bundles_beforewm.fetch_add(1, memory_order_relaxed);
					return bundleptr;
				}
			} else { /* normal transform type */
				auto bundleptr = t->getOneBundleOlderThan(target_wm_, &has_pending_punc, node);
				if (bundleptr) {
					*ptrans = t;
					num_bundles_beforewm.fetch_add(1, memory_order_relaxed);
					return bundleptr;
				}
			}
		}//end for
//  	}
  	// debug
//  	EE("can't get bundle before wm ");

  	/* can't get bundle before target_wm. we'll take any bundle available */
  	for (auto it = this->_transforms.rbegin();
  				it != this->_transforms.rend(); it++)
  	{
#if 0
  		auto bundleptr = (*it)->getOneBundle(node);
  		if (bundleptr) {
				*ptrans = (*it);

#if 0
				{ /* dbg */
					auto & punc = bundleptr->container->getPuncSafe();
					if (punc)
						EE("got a bundle after target wm (%s). from %s. wm %s",
								to_simple_string(target_wm_).c_str(),
								(*ptrans)->name.c_str(),
								to_simple_string(punc->min_ts).c_str());
					else
						EE("got a bundle after target wm (%s). from %s.",
								to_simple_string(target_wm_).c_str(),
								(*ptrans)->name.c_str());
				}
#endif
				num_bundles_afterwm.fetch_add(1, memory_order_relaxed);
				return bundleptr;
		}
#endif
		///////////////////////////////////////////////////
		// type 5: join's downstream's downstream	
		if((*it)->get_side_info() == 5){
			auto bundleptr = (*it)->getOneBundle_5(node);
			if (bundleptr) {
				//std::cout << __FILE__ << ":" << __LINE__ << " get one bundle  for " << (*it)->getName() << std::endl;
				*ptrans = *it;
				num_bundles_afterwm.fetch_add(1, memory_order_relaxed);
				return bundleptr;
			}

		//type 4 trans: Join's downstream
		} else if((*it)->get_side_info() == 4){
			auto bundleptr = (*it)->getOneBundle_4(node);
			if (bundleptr) {
				//std::cout << __FILE__ << ":" << __LINE__ << " get one bundle  for " << (*it)->getName() << std::endl;
				*ptrans = *it;
				num_bundles_afterwm.fetch_add(1, memory_order_relaxed);
				return bundleptr;
			}
		//type 2 trans: join
		}else if((*it)->get_side_info() == 3){
			/* XXX xzl: this is REALLY nasty */

			shared_ptr<BundleBase> bundleptr;
			Join<pair<long, long>>* jt = dynamic_cast<Join<pair<long, long>> *>(*it);
			if (jt) {
				 bundleptr = jt->getOneBundle(node);
			} else {
				Join<pair<long, long>, RecordBundle, RecordBundle>* jt1
										= dynamic_cast<Join<pair<long, long>, RecordBundle, RecordBundle> *>(*it);
				xzl_bug_on(!jt1);  /* what types can Join be? */
				bundleptr = jt1->getOneBundle(node);
			}
			//auto bundleptr = jt->getOneBundleOlderThan(target_wm_, &has_pending_punc, node);
//			bundleptr = jt->getOneBundle(node);

			if (bundleptr) {
				*ptrans = *it;
				num_bundles_afterwm.fetch_add(1, memory_order_relaxed);
				return bundleptr;
			}
		}else{
			//std::cout << __FILE__ << ": " <<  __LINE__ << std::endl;
			auto bundleptr = (*it)->getOneBundle(node);

			if (bundleptr) {
				//std::cout << __FILE__ << ":" << __LINE__ << " get one bundle  for " << (*it)->getName() << std::endl;
				*ptrans = *it;
				num_bundles_afterwm.fetch_add(1, memory_order_relaxed);
				return bundleptr;
			}
		}
		
		///////////////////////////////////////////////////
  	}

#if 0
  	for (auto && t : this->_transforms) {
  		auto bundleptr = t->getOneBundle(node);
			if (bundleptr) {
				*ptrans = t;
				I("got a bundle after target wm...");
				return bundleptr;
			}
  	}
#endif

  	// debug
#if 0
  	EE("can't get bundle at all. ");
  	for (auto && t : this->_transforms) {
//  		cout << t->name <<  ":" << t->getNumBundles() << " ";
//  		t->dump_containers();
  		cout << t->getContainerInfo();
  	}
#endif

//  	EE("can't get bundle at all. ");

//  	assert(false && "todo");

  	/* XXX should work on bundles behind the target wm? */

  	return nullptr;
  }


#if 0
  /*
   * the in-house tp implementation
   */

private:
	vector<pthread_t> threads_;

	static std::condition_variable cv_;
	static std::mutex cv_mtx_; 				/* only for protecting the cv */
	static long nwait_ = 0;

	/* the thread main func */
	static void * run(void *p) {
		while (true) {
			/* examine bundle container in order, consume it */

			/* no work to do. wait to be signaled */
			{
				std::unique_lock<mutex> lck(cv_mtx_);
				nwait_ ++;

				cv_.wait(lck, [] { return true; } );

				/* being woken up and have the lock */
				nwait_ --;
			}
			/* unlocked */

		}
	}

public:
	/* fire up all threads */
	bool StartThreads() {
		for (unsigned int i = 0; i < threads_.size(); i++) {
			int rc = pthread_create(&threads_[i], NULL, run, NULL);
			assert(rc == 0);
		}
		EE("%d threads fired up...");

		return true;
	}

	void StopThreads() {
		assert(false && "not implemented");
	}
#endif

};

#endif // EVAL_BUNDLE_CONTEXT_H


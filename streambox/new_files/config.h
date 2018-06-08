/*
 * config.h
 *
 *  Created on: Nov 1, 2016
 *      Author: xzl
 *
 *  Purdue University, 2016
 */

#ifndef CONFIG_H_
#define CONFIG_H_

#ifdef NDEBUG /* release build */
	#define USE_TBB_DS  1   /* using tbb's concurrent d/s? */
//	#define USE_FOLLY_VECTOR 1 /* use fb's folly vector or std::vector? */
//	#define USE_FOLLY_STRING 1 /* use fb's folly string or std::string? */

//	#define USE_FOLLY_HASHMAP 1
//	#define USE_CUCKOO_HASHMAP 1
	#define USE_TBB_HASHMAP 1

//	#define USE_NUMA_ALLOC 1 /* numa-aware allocation. using Kaskade::Kalloc */

	#define INPUT_ALWAYS_ON_NODE0	1 /* input queue only for node0, i.e no numa? */
	#define MEASURE_LATENCY	1

#else  /* debug build */
	//#define DEBUG 	/* make code easier... */
	#undef DEBUG
	#define USE_TBB_DS  1   /* using tbb's concurrent d/s? */

	/* does not compile when the following two on. why?
	 * https://github.com/facebook/folly/issues/303
	 * https://github.com/facebook/folly/pull/214
	 */
//	#define USE_FOLLY_VECTOR 1 /* use fb's folly vector or std::vector? */
//	#define USE_FOLLY_STRING 1 /* use fb's folly string or std::string? */

//	#define USE_FOLLY_HASHMAP 1
//	#define USE_CUCKOO_HASHMAP 1
	#define USE_TBB_HASHMAP 1

//	#define USE_NUMA_ALLOC 1 /* numa-aware allocation. using Kaskade::Kalloc */

	#define INPUT_ALWAYS_ON_NODE0	1 /* input queue only for node0, i.e no numa? */
	#define MEASURE_LATENCY	1
#endif

// decl in command line
//#define WORKAROUND_JOIN_JDD	1 /* see test-tweet.cpp. must include globally. */

#define CONFIG_NETMON_HT_PARTITIONS 	1024
#define CONFIG_JOIN_HT_PARTITIONS 		4

/* how many threads used by the source? 1 is default; 3 seems okay for intense cases. */
#define CONFIG_SOURCE_THREADS 				1

#define CONFIG_MIN_PERNODE_BUFFER_SIZE_MB		200UL  /* around 3x cache? */
#define CONFIG_MIN_EPOCHS_PERNODE_BUFFER		2UL /* how many epochs before the input buffer wraps around? */

/* -- ARM specific -- */
#define CONFIG_MAX_NUM_CORES 					16		/* the workspace d/s depends on this */
#define CONFIG_MAX_BUNDLES_PER_EPOCH 	32 		/* how many bundles per epoch? */

#define CONFIG_MEMBLOCK_SIZE		SZ_2M  /* per thread memblock */

#endif /* CONFIG_H_ */


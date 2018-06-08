#ifndef EXECUTIONCONTEXT_H
#define EXECUTIONCONTEXT_H

/*
 * ExecutionContext.h
 *
 *  Created on: Jul 1, 2016
 *      Author: xzl
 */
#include <string>
using namespace std;

namespace execution_context {

	class StepContext {
	public:
		virtual string getStepName() = 0;
		virtual string getTransformName() = 0;

	    /**
	     * Hook for subclasses to implement that will be called whenever
	     * {@link org.apache.beam.sdk.transforms.DoFn.Context#output}
	     * is called.
	     */
	    void noteOutput(WindowedValue* output);
		virtual ~StepContext() { }
	};

	class ExecutionContext {
	public:
	    virtual void noteOutput(WindowedValue* output) = 0;
	    virtual vector<StepContext *>* getAllStepContexts() = 0;

	    virtual StepContext * getOrCreateStepContext
	    	(string stepName, string transformName);

	    virtual ~ExecutionContext() { }
	};
}



#endif /* EXECUTIONCONTEXT_H */

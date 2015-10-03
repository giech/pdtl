/*
 * PDTL: Parallel and Distributed Triangle Listing for Massive Graphs
 * Ilias Giechaskiel, George Panagopoulos, Eiko Yoneki
 * 44th International Conference on Parallel Processing (ICPP), Beijing 2015
 * 
 * DOI: 10.1109/ICPP.2015.46
 * 
 * https://github.com/giech/pdtl
 */


#include "util.h"
#include "degreehandler.h"
#include "adjacencyhandler.h"
#include "parserutil.h"
#include "fileparser.h"
#include "mgt.h"
#include "threadpool.h"
#include "networkutil.h"

ThreadPool::ThreadPool(bool outputb, const ThreadInfo& threadinfo)
: output(outputb), info(threadinfo) 
{
	chunk = 0;
	count = 0;
	instances = info.getthreads();
	threads = new std::thread[instances];
	unsigned i;
	for(i = 0; i < instances; i++)
		threads[i] = std::thread(&ThreadPool::execute, this);
	for(i = 0; i < instances; i++)
		threads[i].join();
	delete[] threads;
}

void ThreadPool::execute(void)
{
	unsigned long long localcount = 0;
	unsigned long long index, size = info.getsize();
	while(true)
	{
		chunk_mtx.lock();
		if(chunk == size) {
			chunk_mtx.unlock();
			break;
		}
		index = chunk;
		chunk++;
		chunk_mtx.unlock();
		unsigned long long low = info.getchunks()[index];
		unsigned long long high = info.getchunks()[index+1];
		size_t mem = ((unsigned long long)(info.getmem()))*size
			*(high-low)/
			info.getgraphsize(); // split memory according to number of edges
		MGTAdjacencyHandler *handler = 
			new MGTAdjacencyHandler(info.getinput(),
					info.getmaxDeg(),
					mem,
					output ? getName(getOutName(info.getinput()), index).c_str() : NULL,
					info.getavdegree()[index]);
		handler->timedProcessAdjacency(low, high);
		localcount += handler->getTriangleCount();
		delete handler;
	}
	count_mtx.lock();
	count += localcount;
	count_mtx.unlock();
}

unsigned long long ThreadPool::getcount(void)
{
	return count;
}


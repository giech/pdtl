/*
 * PDTL: Parallel and Distributed Triangle Listing for Massive Graphs
 * Ilias Giechaskiel, George Panagopoulos, Eiko Yoneki
 * 44th International Conference on Parallel Processing (ICPP), Beijing 2015
 * 
 * DOI: 10.1109/ICPP.2015.46
 * 
 * https://github.com/giech/pdtl
 */

#pragma once

// Code that is responsible for thread-related abstractions

#include "util.h"
#include "degreehandler.h"
#include "adjacencyhandler.h"
#include "parserutil.h"
#include "fileparser.h"
#include "mgt.h"
#include "loadbalance.h"
#include <mutex>
#include <thread>

class ThreadPool
{
	public:
		ThreadPool(bool outputb, const ThreadInfo& threadinfo);
		unsigned long long getcount(void);

	private:
		void execute(void);
		unsigned instances;
		bool output;
		const ThreadInfo& info;
		std::mutex chunk_mtx;
		unsigned chunk;
		std::mutex count_mtx;
		unsigned long long count;
		std::thread *threads;
};


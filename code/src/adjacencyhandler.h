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

#include "util.h"
#include "degreehandler.h"
#include "parserutil.h"

// class that transparently takes care of going through adjacency file once

class AdjacencyHandler {
	public:
		// have 2 of bufferSize*sizeof(vx) (one for buffer, one for DegreeHandler)
		AdjacencyHandler(const std::string file, size_t bufferSize = DEFAULT_BUF);
		AdjacencyHandler(const std::string file, DegreeHandler* deg, 
				             size_t bufferSize = DEFAULT_BUF);
		virtual ~AdjacencyHandler();
		void processAdjacency(unsigned long long low, unsigned long long high);

	protected:
		DegreeHandler* deg;
		FILE* fd;
		size_t graphSize;
	private:
		size_t bufferSize;
		vx* buffer;

		bool own;
		void init(const std::string file, size_t bufferSize);


		virtual void overallSetUp() = 0;
		virtual void processPhase() = 0;
		virtual void phaseSetUp() = 0;
		virtual bool handleEdge(vx from, vx to, vx fromDeg) = 0;
		virtual void overallTearDown() = 0;
};


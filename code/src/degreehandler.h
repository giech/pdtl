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

// class that makes degree handling transparent

class DegreeHandler{
	public:
		DegreeHandler(const std::string file, size_t bufferSize = DEFAULT_BUF);
		virtual ~DegreeHandler();
		virtual vx getDegree(vx v);
		size_t getGraphSize();

	protected:
		FILE* fd;
	private:
		vx low;
		vx high;
		vx graphSize;
		size_t bufferSize;
		vx* degrees;

		bool updateDegrees(vx lower);
};

class NonSequentialDegreeHandler : public DegreeHandler
{
	public:
		NonSequentialDegreeHandler(const std::string file);
		~NonSequentialDegreeHandler();
		virtual vx getDegree(vx x);
};


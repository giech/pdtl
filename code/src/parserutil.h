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
#include "filebuffer.h"

// Utility to write the two files efficiently
// Call close before destruction

class ParserUtil {
	public:
		// bufferSize for each of the two FileBuffers
		ParserUtil(const std::string base, 
		           size_t bufferSize = DEFAULT_BUF,
				       bool tofillstart = true, 
				       bool tofillend = true);
		~ParserUtil();

		vx getMaxDegree();
		void addEdge(vx from, vx to);
		void close();

	private:
		FileBuffer* deg;
		FileBuffer* adj;
		vx maxVx;
		vx maxDeg;
		vx current;
		vx prev;
		vx total;
		void writeDegreePair(vx index, vx deg);
		void writeGap();
		void writeEdge(vx to);
		bool closed, fillstart, fillend;
};


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

// Buffer class so that we only write to disk in batches.
// Close before destruction.

class FileBuffer {
	public:
		FileBuffer(const std::string file, size_t bufferSize = DEFAULT_BUF);
		~FileBuffer();
		void addToBuffer(vx v);
		void addToBuffer(vx* v, size_t size);
		void close();

	private:
		FILE* out;
		vx* buffer;
		size_t bufferSize;
		size_t bufferIndex;
		bool closed;
		void flush();
};


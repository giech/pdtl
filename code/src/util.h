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

// Utility file for common definitions
// We also re-define fseek to work with large files

#define fseek64 fseeko64
#define TIMEVAL_TO_SEC(time) ((double) time.tv_sec + (double)time.tv_usec * .000001)

#include <string>
#include <algorithm>
#include <iostream>
#include <cstring>
#include <atomic>

#ifdef BIT64
typedef unsigned long long vx;
#else
typedef unsigned int vx;
#endif

#define UNINIT ((vx) -1)
#define MAX_EDGES ((unsigned long long) -1)

#define MB_TO_B (1024*1024/sizeof(vx))

#ifndef DEFAULT_BUF
#define DEFAULT_BUF (4*MB_TO_B)
#endif

#define READ_FLAG "rb"
#define WRITE_FLAG "wb"

std::string getAdjName(const char* base);
std::string getDegName(const char* base);
std::string getOutName(const char* base);

size_t getFileSize(const char* file);

vx processIntersection(vx* first, vx firstSize, vx* second, vx secondSize, vx* out);
vx mergeIntersection(vx* first, vx firstSize, vx* second, vx secondSize, vx* out);
vx fastIntersection(vx* first, vx firstSize, vx* second, vx secondSize, vx* out);


class Timer {
	public:
		void start();
		void reset();
		double lap();
		double total();

	private:
		double start_time;
		double last_time;
		double getTime();
};


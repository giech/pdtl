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

// defines parser utilities


vx undirect(const char* input, const char* output);
vx orient(const char* input, const char* output, size_t degMB = 0, 
          unsigned threads = 1);
void orderNeighbors(const char* input, const char* output);


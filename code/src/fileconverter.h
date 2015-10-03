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

void parseAdjacencyList(const char* input, const char* output, int starter = 0);
void parseXStream(const char* input, const char* output, size_t mem=0, unsigned int vn=3);
void convertToXStream(const char* input, const char* output);
void convertToOPT(const char* input, const char* output);

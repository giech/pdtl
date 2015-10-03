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

#include "adjacencyhandler.h"
#include "filebuffer.h"
#include "parserutil.h"
#include <unordered_set>

// Class to handle high-degree nodes

class HighDegreeHandler  : public AdjacencyHandler {
	public:
		HighDegreeHandler(const std::string input, 
		                  const std::string output, 
		                  vx maxDeg, 
		                  const char* triangles = NULL, 
		                  unsigned int bufferSize = DEFAULT_BUF);
		virtual ~HighDegreeHandler();

		void handle();

		unsigned long long getTriangleCount();


	private:
		std::string outName;
		std::string tempName;
		FileBuffer* out;
		ParserUtil* parser;
		vx* adj;
		std::unordered_set<vx>* adj_set;
		std::unordered_set<vx>::iterator adj_end;

		vx lastFrom;
		vx* neighbors;
		vx neighborSize;

		vx* intersection;

		unsigned long long triangleCount;

		unsigned long long offset;
		vx maxDeg;
		vx highDegVx;
		bool containedHigh;

		vx findHighDegVx();
		void copyFiles(const std::string from, const std::string to, vx bufferSize);
		void copyFile(const std::string from, 
		              const std::string to, 
		              vx* buffer, 
		              vx bufferSize);
		void createHashStructures();
		void resetFiles();
		void handleEdge(vx from, vx to, vx degree, ParserUtil* parser);
		void handleSet();
		void reportTriangle(vx u, vx v, vx w);

		virtual void overallSetUp();
		virtual void processPhase();
		virtual void phaseSetUp();
		virtual bool handleEdge(vx from, vx to, vx fromDeg);
		virtual void overallTearDown();
};


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
#include "adjacencyhandler.h"

// class which implements the MGT algorithm with our modifications

class MGTAdjacencyHandler : public AdjacencyHandler {
 public:
  MGTAdjacencyHandler(const std::string input,
                      vx mxDg, 
                      unsigned long long totalMem, 
                      const char* output, 
                      double avdegree, 
                      unsigned int bufferSize = DEFAULT_BUF);
  virtual ~MGTAdjacencyHandler();
  unsigned long long getTriangleCount();
  void timedProcessAdjacency(unsigned long long low, unsigned long long high);
 private:
  FILE* adjFd;
  vx maxDeg;

  vx* nmem;
  vx nmemsize;

  vx* nmemplus;
  vx nmemplussize;

  vx* intersection;

  FileBuffer* b;

  vx bufferOffset;
  vx remainingEdges;
	
  unsigned long long triangleCount;
  vx lastFrom;

  vx* vxBuffer;
  vx vxBufferSize;

  unsigned long long* inds;
  vx lowIndex;
  vx newLowIndex;
  unsigned long long sizeIndex;

  vx* edges;
  unsigned long long sizeEdges;
  unsigned long long curEdge;
  Timer t;

  virtual void overallSetUp();
  virtual void processPhase();
  virtual void phaseSetUp();
  virtual bool handleEdge(vx from, vx to, vx fromDeg);
  virtual void overallTearDown();


  void createVertexStructures(vx from);
  void updateBuffer(bool rewind);
  vx getIndex(vx from);
  bool isValidIndex(vx from);
};

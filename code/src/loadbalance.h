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
#include "adjacencyhandler.h"
#include "parserutil.h"
#include "fileparser.h"
#include "mgt.h"

// Code that is responsible for load-balancing of graph to each core

class ThreadInfo
{
public:
  ThreadInfo(const char *inputfile,
	     const size_t& memory, const vx& maxdegree,
	     const vx& numthreads);

  ~ThreadInfo();

  virtual void loadbalance(void) = 0;
  inline size_t getmem() const { return mem; }
  inline vx getmaxDeg() const { return maxDeg; }
  inline const unsigned long long *getchunks() const { return chunks; }
  inline unsigned getsize() const { return size; }
  inline const char* getinput() const { return input.c_str(); }
  inline vx getthreads() const { return threads; }
  inline unsigned long long getgraphsize() const { return graphSize; }
  inline const double *getavdegree() const { return avdegree; }

protected:
  std::string input;
  size_t mem;
  vx maxDeg;
  unsigned long long graphSize;
  vx threads;
  unsigned long long *chunks;
  double *avdegree;
  unsigned size;
};

class ThreadCoefficient : public ThreadInfo
{
public:
  ThreadCoefficient(const char *inputfile,
		    const size_t& memory, const vx& maxdegree, 
		    const vx& numthreads, double coefficient);
  virtual void loadbalance();

private:
  double coeff;
};

class ThreadLinear : public ThreadInfo
{
public:
  ThreadLinear(const char *inputfile,
	       const size_t& memory, const vx& maxdegree,
	       const vx& numthreads);
  virtual void loadbalance();
};

class Volume : public ThreadInfo
{
public:
  Volume(const char *inputfile,
	 const size_t& memory, const vx& maxdegree,
	 const vx& numthreads, unsigned sizen, const char *degrees);
  virtual void loadbalance();

private:
  DegreeHandler ordeg, nonordeg;
};

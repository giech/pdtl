/*
 * PDTL: Parallel and Distributed Triangle Listing for Massive Graphs
 * Ilias Giechaskiel, George Panagopoulos, Eiko Yoneki
 * 44th International Conference on Parallel Processing (ICPP), Beijing 2015
 * 
 * DOI: 10.1109/ICPP.2015.46
 * 
 * https://github.com/giech/pdtl
 */


#include "util.h"
#include "degreehandler.h"
#include "adjacencyhandler.h"
#include "parserutil.h"
#include "fileparser.h"
#include "mgt.h"
#include "loadbalance.h"
#include "assert.h"

ThreadInfo::ThreadInfo(const char *inputfile, 
                       const size_t& memory, 
		                   const vx& maxdegree, 
		                   const vx& numthreads)
: input(inputfile), mem(memory), maxDeg(maxdegree), threads(numthreads) 
{
	graphSize = getFileSize(getAdjName(input.c_str()).c_str())/sizeof(vx);
	chunks = NULL;
	avdegree = NULL;
}

ThreadInfo::~ThreadInfo()
{
	if(chunks != NULL)
		delete[] chunks;
	if(avdegree != NULL)
		delete[] avdegree;
}

ThreadCoefficient::ThreadCoefficient(const char *inputfile,
		                                 const size_t& memory, 
		                                 const vx& maxdegree,
		                                 const vx& numthreads, 
		                                 double coefficient)
: ThreadInfo(inputfile, memory, maxdegree, numthreads), coeff(coefficient)
{
	size = threads*coeff;
	chunks = new unsigned long long[size+1];
}

void ThreadCoefficient::loadbalance(void)
{
	unsigned long long diff = graphSize/size;
	unsigned long long prev = 0;
	for(unsigned long i = 0; i < size; i++)
	{
		chunks[i] = prev;
		prev += diff;
	}
	chunks[size] = graphSize;
}

ThreadLinear::ThreadLinear(const char *inputfile,
		const size_t& memory, const vx& maxdegree,
		const vx& numthreads)
: ThreadInfo(inputfile, memory, maxdegree, numthreads)
{
	size = threads;
	chunks = new unsigned long long[size+1];
}

void ThreadLinear::loadbalance(void)
{
	unsigned long long diff = graphSize/size;
	unsigned long long prev = 0;
	for(unsigned long i = 0; i < size; i++)
	{
		chunks[i] = prev;
		prev += diff;
	}
	chunks[size] = graphSize;
}

Volume::Volume(const char *inputfile,
		           const size_t& memory, 
		           const vx& maxdegree,
		           const vx& numthreads, 
		           unsigned sizen, 
		           const char *degrees)
: ThreadInfo(inputfile, memory, maxdegree, numthreads),
  ordeg(getDegName(inputfile).c_str()), 
	nonordeg(getDegName(degrees).c_str())
{
	size = sizen;
	chunks = new unsigned long long[size+1];
	avdegree = new double[size];
}

void Volume::loadbalance()
{
	vx maxver = getFileSize(getDegName(input.c_str()).c_str())
		/sizeof(vx)/2;
	if(size == 1)
	{
		chunks[0] = 0;
		chunks[1] = graphSize;
		avdegree[0] = (double) graphSize/(double)maxver;
		return;
	}
	unsigned long long interval = graphSize*(sizeof(unsigned long long)+sizeof(vx))/mem/1024/1024/threads + 1,
		      nointervals = mem*threads*1024*1024/(sizeof(unsigned long long)+sizeof(vx));
	unsigned long long *cumdegree = new unsigned long long[nointervals];
	vx *verat = new vx[nointervals];
	unsigned long long counter, index, total;
	counter = total = index = 0;
	for(vx ver = 0; ver < maxver; ver++)
	{
		unsigned long long fromDeg = ordeg.getDegree(ver);
		if(!fromDeg) continue;
		unsigned long long nonFromDeg = nonordeg.getDegree(ver),
			      value = nonFromDeg - fromDeg + 1;
		if(counter + fromDeg < interval)
		{
			counter += fromDeg;
			total += fromDeg*value;
		} else
		{
			total += (interval - counter)*value;
			cumdegree[index] = total;
			verat[index] = ver;
			index++;
			fromDeg = fromDeg + counter - interval;
			while(fromDeg >= interval)
			{
				total += interval*value;
				cumdegree[index] = total;
				verat[index] = ver;
				index++;
				fromDeg -= interval;
			}
			counter = fromDeg;
			total += fromDeg*value;
		}
	}

	if(counter)
	{
		cumdegree[index] = total;
		verat[index] = maxver;
		index++;
	}
	nointervals = index;

	unsigned long long perthread = total/size;
	unsigned long long i = 0, j, prev = 0, prevTot = 0;
	vx prevver = 0;
	chunks[0] = 0;
	for(j = 1; j < size; j++)
	{
		for(; i < nointervals; i++)
			if(cumdegree[i] >= prevTot + perthread)
				break;
		prev = i*interval;
		prevTot = cumdegree[i];
		chunks[j] = prev;
		avdegree[j-1] = (double) (chunks[j]-chunks[j-1])/(double)(verat[i]-prevver);
		prevver = verat[i];
	}
	chunks[size] = graphSize;
	avdegree[size-1] = (double) (chunks[size]-chunks[size-1])/(double)(maxver-prevver);
	delete[] verat;
	delete[] cumdegree;
}


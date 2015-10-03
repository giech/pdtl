/*
 * PDTL: Parallel and Distributed Triangle Listing for Massive Graphs
 * Ilias Giechaskiel, George Panagopoulos, Eiko Yoneki
 * 44th International Conference on Parallel Processing (ICPP), Beijing 2015
 * 
 * DOI: 10.1109/ICPP.2015.46
 * 
 * https://github.com/giech/pdtl
 */

#include <sys/resource.h>

#include "util.h"
#include "degreehandler.h"
#include "adjacencyhandler.h"
#include "parserutil.h"
#include "fileparser.h"
#include "mgt.h"

using namespace std;

	MGTAdjacencyHandler::MGTAdjacencyHandler(const std::string input, 
	                                         vx mxDg, 
	                                         unsigned long long totalMem, 
	                                         const char* output, 
	                                         double avdegree, 
	                                         unsigned int bufferSize)
: AdjacencyHandler(input, bufferSize)
{
	unsigned long long remainingMem = totalMem*MB_TO_B;
	const char* input_str = input.c_str();

	string adjName = getAdjName(input_str);
	adjFd = fopen(adjName.c_str(), READ_FLAG);
	vxBufferSize = bufferSize;
	vxBuffer = new vx[vxBufferSize];

	maxDeg = mxDg;
	nmem = new vx[maxDeg];
	nmemplus = new vx[maxDeg];

	// adj and deg for super + vxBuf + variables
	unsigned long long bufferTotals = 2*bufferSize + vxBufferSize + 2*maxDeg + 100;

	if (output != NULL)
	{
		string outName(output);
		if (outName.size() == 0)
		{
			outName = getOutName(input_str);
		}
		b = new FileBuffer(outName, bufferSize);
		intersection = new vx[maxDeg];
		bufferTotals += bufferSize + maxDeg;
	}
	else
	{
		intersection = NULL;
		b = NULL;
	}

	if(bufferTotals > remainingMem)
		remainingMem = 0;
	else
		remainingMem -= bufferTotals;
	if(remainingMem > totalMem*MB_TO_B)
		remainingMem = 0;
	unsigned long long index = (unsigned long long)(remainingMem/(avdegree+2)); // for each vertex we have avdegree
	if(index == 0)
		index = 1;
	sizeIndex = index;
	inds = new unsigned long long[2*sizeIndex];

	sizeEdges = avdegree*index;
	if(sizeEdges == 0)
		sizeEdges = 1;
	edges = new vx[sizeEdges];
}

MGTAdjacencyHandler::~MGTAdjacencyHandler()
{
	delete[] nmem;
	delete[] nmemplus;
	delete[] vxBuffer;
	delete[] inds;
	delete[] edges;

	if (b != NULL)
	{
		delete[] intersection;
		b->close();
		delete b;
	}

	fclose(adjFd);
}

unsigned long long MGTAdjacencyHandler::getTriangleCount()
{
	return triangleCount;
}

inline vx MGTAdjacencyHandler::getIndex(vx i)
{
	return 2*(i - lowIndex);
}

inline bool MGTAdjacencyHandler::isValidIndex(vx i)
{
	return (lowIndex <= i) && (i < lowIndex + sizeIndex);
}

void MGTAdjacencyHandler::overallSetUp()
{
	triangleCount = 0;
	newLowIndex = 0;
	t.start();
}

void MGTAdjacencyHandler::processPhase()
{
	cout << "Starting reporting after " << t.lap() << endl;
	updateBuffer(true);

	// for each vertex u in V
	vx u;
	for (u = 0; u < graphSize; ++u)
	{
		// create Nmem(u), Nmem+(u)
		createVertexStructures(u);

		// for each v in Nmem+(u)
		vx vi;
		for (vi = 0; vi < nmemplussize; ++vi)
		{
			vx v = nmemplus[vi];
			vx index = getIndex(v);

			vx dg = inds[index];
			if (dg == UNINIT)
			{
				cout << "INTERNAL ERROR" << endl;
				continue;
			}

			vx* adj = edges + inds[index + 1];
			vx interSize = processIntersection(nmem, nmemsize, adj, dg, intersection);
			triangleCount += interSize;
			if (b != NULL)
			{
				vx i;
				for (i = 0; i < interSize; ++i)
				{
					b->addToBuffer(u);
					b->addToBuffer(v);
					b->addToBuffer(intersection[i]);
				}
			}
		}
	}

	cout << "Triangles after phase: " << triangleCount << endl;
	cout << "Phase took " << t.lap() << endl;
}

void MGTAdjacencyHandler::phaseSetUp()
{
	fill(inds, inds + 2*sizeIndex, UNINIT);

	lastFrom = UNINIT;
	curEdge = 0;
	lowIndex = newLowIndex;
}

void MGTAdjacencyHandler::overallTearDown()
{

}

bool MGTAdjacencyHandler::handleEdge(vx from, vx to, vx degree)
{
	if (!isValidIndex(from) || curEdge == sizeEdges)
	{
		newLowIndex = from;
		return false;
	}

	vx index = getIndex(from);
	if (from != lastFrom)
	{
		inds[index] = 0;
		inds[index + 1] = curEdge;
		lastFrom = from;
	}

	++inds[index];	
	edges[curEdge] = to;
	++curEdge;

	return true;
}

void MGTAdjacencyHandler::updateBuffer(bool rewind)
{
	if (rewind)
	{
		fseek64(adjFd, 0, SEEK_SET);
	}
	remainingEdges = (vx) fread(vxBuffer, sizeof(vx), vxBufferSize, adjFd);
	bufferOffset = 0;
}

void MGTAdjacencyHandler::createVertexStructures(vx from)
{
	vx degree = deg->getDegree(from);

	nmemsize = 0;
	nmemplussize = 0;

	vx left = degree;
	while (left > 0)
	{
		if (remainingEdges == 0)
		{
			updateBuffer(false);
		}

		vx toRead = remainingEdges;
		if (toRead > left)
		{
			toRead = left;
		}

		vx u;
		for (u = 0; u < toRead; ++u)
		{
			vx to = vxBuffer[bufferOffset + u];

			nmem[nmemsize] = to;
			++nmemsize;
			if (isValidIndex(to) && inds[getIndex(to)] != UNINIT)
			{
				nmemplus[nmemplussize] = to;
				++nmemplussize;
			}
		}

		left -= toRead;
		remainingEdges -= toRead;
		bufferOffset += toRead;

	}
}

void MGTAdjacencyHandler::timedProcessAdjacency(unsigned long long low, 
                                                unsigned long long high)
{
	Timer t;
	t.start();
	struct rusage usage;
	getrusage(RUSAGE_THREAD, &usage);
	double before_user = TIMEVAL_TO_SEC(usage.ru_utime);
	double before_system = TIMEVAL_TO_SEC(usage.ru_stime);
	processAdjacency(low, high);
	getrusage(RUSAGE_THREAD, &usage);
	double total = t.total();
	cout << "Thread exit; triangles = " << triangleCount
		<< ", total time = " << total
		<< ", approx. I/O time = " << total -
		(TIMEVAL_TO_SEC(usage.ru_utime) - before_user +
		 TIMEVAL_TO_SEC(usage.ru_stime) - before_system)
		<< ", soft page faults " << usage.ru_minflt
		<< ", hard page faults " << usage.ru_majflt
		<< ", edges " << low << " to " << high << endl;
}


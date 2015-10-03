/*
 * PDTL: Parallel and Distributed Triangle Listing for Massive Graphs
 * Ilias Giechaskiel, George Panagopoulos, Eiko Yoneki
 * 44th International Conference on Parallel Processing (ICPP), Beijing 2015
 * 
 * DOI: 10.1109/ICPP.2015.46
 * 
 * https://github.com/giech/pdtl
 */

#include "adjacencyhandler.h"

using namespace std;

AdjacencyHandler::AdjacencyHandler(const string file, size_t size)
{
	init(file, size);
	string degName = getDegName(file.c_str());
	deg = new DegreeHandler(degName, bufferSize);
	own = true;
	graphSize = deg->getGraphSize();
}

AdjacencyHandler::AdjacencyHandler(const std::string file, 
                                   DegreeHandler* dgr, 
                                   size_t size)
{
	init(file, size);
	if (dgr != NULL)
	{
		deg = dgr;
		own = false;
	}
	else
	{
		string degName = getDegName(file.c_str());
		deg = new DegreeHandler(degName, bufferSize);
		own = true;
	}
	graphSize = deg->getGraphSize();
}

void AdjacencyHandler::init(const std::string file, size_t size)
{
	bufferSize = size;
	buffer = new vx[bufferSize];

	const char* name = file.c_str();

	string adjName = getAdjName(name);
	fd = fopen(adjName.c_str(), READ_FLAG);
}

AdjacencyHandler::~AdjacencyHandler()
{
	delete[] buffer;
	if (own)
	{
		delete deg;
	}
	fclose(fd);
}

void AdjacencyHandler::processAdjacency(unsigned long long low, 
                                        unsigned long long high)
{
	fseek64(fd, low*sizeof(vx), SEEK_SET);

	vx u = 0;
	unsigned long long off = 0;
	while (off <= low)
	{
		off += deg->getDegree(u);
		++u;
	}


	vx vertex = u - 1;
	vx processed = deg->getDegree(vertex) + (vx) (low - off);
	unsigned long long rem = high - low;
	unsigned long long buffSize = bufferSize;
	size_t size;

	overallSetUp();
	phaseSetUp();
	while (0 < (size = fread(buffer, sizeof(vx), min(rem, buffSize), fd)))
	{
		size_t total = 0;

		while (total < size)
		{
			vx degree = deg->getDegree(vertex);
			vx remaining = degree - processed;
			vx from = vertex;

			if (remaining > size - total)
			{
				remaining = (vx) (size - total);
				processed += remaining;
			}
			else
			{
				++vertex;
				processed = 0;
			}

			vx i;

			for (i = 0; i < remaining; ++i)
			{
				vx to = buffer[total + i];
				while (!handleEdge(from, to, degree))
				{
					processPhase();
					phaseSetUp();
				}

			}

			total += remaining;

		}

		rem -= (unsigned long long) size;
	}


	processPhase();
	overallTearDown();
}


/*
 * PDTL: Parallel and Distributed Triangle Listing for Massive Graphs
 * Ilias Giechaskiel, George Panagopoulos, Eiko Yoneki
 * 44th International Conference on Parallel Processing (ICPP), Beijing 2015
 * 
 * DOI: 10.1109/ICPP.2015.46
 * 
 * https://github.com/giech/pdtl
 */

#include "degreehandler.h"
#include <cassert>
using namespace std;

#define BYTE_OFFSET (2*sizeof(vx))

DegreeHandler::DegreeHandler(const std::string file, size_t bufSize)
{
	bufferSize = bufSize;
	if (bufferSize < 2)
	{
		throw 1;
	}

	const char* filename = file.c_str();
	fd = fopen(filename, READ_FLAG);
	graphSize = (getFileSize(filename)/BYTE_OFFSET);
	degrees = new vx[bufferSize];
	updateDegrees(0);

	// TODO: Check return values
}


DegreeHandler::~DegreeHandler()
{
	delete[] degrees;
	fclose(fd);
}

bool DegreeHandler::updateDegrees(vx lower)
{
	//std::cout << "UPDATING DEG: " << lower << std::endl;
	fseek64(fd, ((size_t) lower)*BYTE_OFFSET, SEEK_SET); // pairs (id, vx)

	size_t size = fread(degrees, sizeof(vx), bufferSize, fd);

	if (size < 2)
	{
		cout << size << " TOO SMALL FOR VERTEX " << lower << endl;
		return false;
	}

	low = degrees[0];
	high = degrees[size - 2];
	return true;
}

vx DegreeHandler::getDegree(vx v)
{
	if (low > v || high < v)
	{
		if (!updateDegrees(v))
		{
			return 0;
		}
	}

	return degrees[2*((size_t)(v - low))+1];
}

size_t DegreeHandler::getGraphSize()
{
	return graphSize;
}

	NonSequentialDegreeHandler::NonSequentialDegreeHandler(const std::string file)
: DegreeHandler(file, 2)
{}

NonSequentialDegreeHandler::~NonSequentialDegreeHandler()
{}

vx NonSequentialDegreeHandler::getDegree(vx x)
{
	vx answer;
	fseek64(fd, sizeof(vx)*((size_t) x * 2 + 1) , SEEK_SET);
	assert(1 == fread(&answer, sizeof(vx), 1, fd));
	return answer;
}


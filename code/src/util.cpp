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

#include <sys/stat.h>

using namespace std;

#define RATIO_CUTOFF 5
#define SUM_CUTOFF 100

// makes Timer implementation transparent

#ifdef _OPENMP
#include <omp.h>
#define CURRENT_TIME() return omp_get_wtime();
#else
#include <sys/time.h>
#define CURRENT_TIME()						\
	struct timeval time;						\
if (gettimeofday(&time,NULL)){ return 0;}			\
return TIMEVAL_TO_SEC(time);
#endif

string getAdjName(const char* base)
{
	string a(base);
	string b(".adj");
	return a + b;
}

string getDegName(const char* base)
{
	string a(base);
	string b(".deg");
	return a + b;
}

string getOutName(const char* base)
{
	string a(base);
	string b(".out");
	return a + b;
}

size_t getFileSize(const char* file)
{
	struct stat filestatus;
	stat(file, &filestatus);
	return filestatus.st_size;
}

vx lowerBound(vx* list, vx size, vx k)
{
	long long lo = 0;
	long long hi = (long long) size;
	long long mid;
	vx val;

	while (lo < hi)
	{
		mid = lo + (hi-lo)/2;
		val = list[mid];
		if (val < k)
		{
			lo = mid +1;
		}
		else
		{
			hi = mid;
		}
	}

	return (vx) lo;
}

vx processIntersection(vx* first, vx firstSize, vx* second, vx secondSize, vx* out)
{
	vx sum = firstSize + secondSize;

	if (sum <= SUM_CUTOFF || sum < RATIO_CUTOFF*min(firstSize, secondSize))
	{
		return mergeIntersection(first, firstSize, second, secondSize, out);
	}
	else
	{
		return fastIntersection(first, firstSize, second, secondSize, out);
	}
}

vx mergeIntersection(vx* first, vx firstSize, vx* second, vx secondSize, vx* out)
{
	vx firstIndex = 0;
	vx secondIndex = 0;
	vx size = 0;

	while (firstIndex < firstSize && secondIndex < secondSize)
	{

		vx uFirst = first[firstIndex];
		vx uSecond = second[secondIndex];
		if (uFirst < uSecond)
		{

			++firstIndex;
		}
		else if (uFirst > uSecond)
		{
			++secondIndex;
		}
		else
		{
			if (out != NULL)
			{
				out[size] = uFirst;
			}
			++size;
			++firstIndex;
			++secondIndex;
		}
	}

	return size;
}


vx fastIntersection(vx* first, vx firstSize, vx* second, vx secondSize, vx* out)
{
	// Code adapted from:
	// https://github.com/erikfrey/themas/tree/master/src/set_intersection/
	if (firstSize == 0 || secondSize == 0)
	{
		return 0;
	}

	vx sum = secondSize + firstSize;
	if (sum <= SUM_CUTOFF || sum < RATIO_CUTOFF*min(firstSize, secondSize))
	{
		return mergeIntersection(first, firstSize, second, secondSize, out);
	}

	if (secondSize < firstSize)
	{
		vx* temp = first;
		vx tempSize = firstSize;

		first = second;
		firstSize = secondSize;

		second = temp;
		secondSize = tempSize;
	}

	vx medianIndex = firstSize/2;
	vx val = first[medianIndex];

	vx lowBound = lowerBound(second, secondSize, val);

	vx left = fastIntersection(first, medianIndex, second, lowBound, out);

	if (lowBound < secondSize && second[lowBound] == val)
	{
		if (out != NULL)
		{
			out[left] = val;
		}
		++left;
		++lowBound;
	}

	++medianIndex;

	vx right = 0;

	if (medianIndex < firstSize  && lowBound < secondSize)
	{
		right = fastIntersection(first + medianIndex, 
		                         firstSize - medianIndex, 
		                         second + lowBound, 
		                         secondSize - lowBound, 
		                         out != NULL ? out + left : out);
	}

	return left + right;
}

void Timer::start()
{
	start_time = getTime();
	last_time = start_time;
}

void Timer::reset()
{
	start();
}

double Timer::getTime()
{
	CURRENT_TIME();
}

double Timer::lap()
{
	double t = getTime();
	double d = t - last_time;
	last_time = t;
	return d;
}

double Timer::total()
{
	return getTime() - start_time;
}


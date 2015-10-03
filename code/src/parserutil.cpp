/*
 * PDTL: Parallel and Distributed Triangle Listing for Massive Graphs
 * Ilias Giechaskiel, George Panagopoulos, Eiko Yoneki
 * 44th International Conference on Parallel Processing (ICPP), Beijing 2015
 * 
 * DOI: 10.1109/ICPP.2015.46
 * 
 * https://github.com/giech/pdtl
 */
 
 
#include "parserutil.h"

using namespace std;

ParserUtil::ParserUtil(const string base, 
                       size_t size, 
                       bool tofillstart,
		                   bool tofillend)
{
  const char* base_str = base.c_str();
  adj = new FileBuffer(getAdjName(base_str), size);
  deg = new FileBuffer(getDegName(base_str), size);
  current = UNINIT;
  maxVx = 0;
  prev = 0;
  total = 0;
  maxDeg = 0;
  closed = false;
  fillstart = tofillstart;
  fillend = tofillend;
}

ParserUtil::~ParserUtil()
{
  if (!closed)
    {
      close();
    }
  delete adj;
  delete deg;
}

void ParserUtil::addEdge(vx from, vx to)
{
  writeEdge(to);
  if (current != from)
    {
      if (current != UNINIT)
	{
	  writeGap();
	} else if(!fillstart)
	current = from - 1;

      prev = current + 1;
      current = from;
      total = 0;
    }
  ++total;

  maxDeg = max(total, maxDeg);
  maxVx = max(maxVx, max(from, to));
}

vx ParserUtil::getMaxDegree()
{
  return maxDeg;
}

void ParserUtil::writeEdge(vx to)
{
  adj->addToBuffer(to);
}

void ParserUtil::writeDegreePair(vx index, vx degree)
{
  deg->addToBuffer(index);
  deg->addToBuffer(degree);
}

void ParserUtil::writeGap()
{
  vx i;
  vx diff = current - prev;

  for (i = 0; i < diff; ++i)
    {
      writeDegreePair(prev + i, 0);
    }

  writeDegreePair(current, total);
}

void ParserUtil::close()
{
  writeGap();

  if(fillend)
    {
      vx i;
      vx diff = maxVx - current;
      for (i = 0; i < diff; ++i)
	{
	  writeDegreePair(current + i + 1, 0);
	}
    }

  adj->close();
  deg->close();
  closed = true;
}

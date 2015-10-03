/*
 * PDTL: Parallel and Distributed Triangle Listing for Massive Graphs
 * Ilias Giechaskiel, George Panagopoulos, Eiko Yoneki
 * 44th International Conference on Parallel Processing (ICPP), Beijing 2015
 * 
 * DOI: 10.1109/ICPP.2015.46
 * 
 * https://github.com/giech/pdtl
 */

#include "fileparser.h"
#include "fileconverter.h"
#include "util.h"

// Main for calling the fileparser and fileconverter utilities

using namespace std;

void printUsage(char* name)
{
	cerr << "Usage: " << name << " method input output [extravalues]" << endl;
	cerr << "Method can only be one of parse, convert, order, undirect, orient" << endl;
	cerr << "Undirect and order do not take extra values." << endl;
	cerr << "parse snap/xstream [mem] [2/3 for xstream]" << endl;
	cerr << "convert opt/xstream" << endl;
	cerr << "orient [mem] [numthreads]" << endl;
}

int main(int argc, char* argv[])
{
	Timer t;
	t.start();
	if (argc < 4 || argc > 7)
	{
		printUsage(argv[0]);
		return 1;
	}

	const char* method = argv[1];
	const char* input = argv[2];
	const char* output = argv[3];

	unsigned threads = 1;
	unsigned long mem = 0;

	if (!strcmp(method, "parse"))
	{
	  const char* type = argv[4];
	  if (argc >= 6)
	  {
	    mem = atoll(argv[5]);
	  }
	  
	  if (!strcmp(type, "snap"))
	  {
	    parseAdjacencyList(input, output, mem);
	  }
	  else if (!strcmp(type, "xstream"))
	  {
	    unsigned int vn=3;
	    if (argc >= 7)
	    {
	      vn = atoi(argv[6]);
	    }
	  
	    parseXStream(input, output, mem, vn);
	  }
	  else
	  {
	    printUsage(argv[0]);
		  return 1;
	  } 
	}
	else if (!strcmp(method, "order"))
	{
		orderNeighbors(input, output);
	}
	else if (!strcmp(method, "undirect"))
	{
		undirect(input, output);
	}
	else if (!strcmp(method, "orient"))
	{
	  if (argc >= 5)
	  {
		  mem = atoll(argv[4]);
	  }
	  if (argc == 6)
		  threads = atoi(argv[5]);
	
		vx maxDeg = orient(input, output, mem, threads);
		cout << "Max degree is " << maxDeg << endl;
	}
	else if (!strcmp(method, "convert"))
	{
	  const char* type = argv[4];
	  if (!strcmp(type, "opt"))
	  {
	    convertToOPT(input, output);
	  }
	  else if (!strcmp(type, "xstream"))
	  {
	    convertToXStream(input, output);
	  }
	  else
	  {
	    printUsage(argv[0]);
		  return 1;
	  } 
	}
	else
	{
		printUsage(argv[0]);
		return 1;
	}
	cout << "Finished " << method << " in " << t.total() << endl;
	return 0;
}


/*
 * PDTL: Parallel and Distributed Triangle Listing for Massive Graphs
 * Ilias Giechaskiel, George Panagopoulos, Eiko Yoneki
 * 44th International Conference on Parallel Processing (ICPP), Beijing 2015
 * 
 * DOI: 10.1109/ICPP.2015.46
 * 
 * https://github.com/giech/pdtl
 */

#include "mgt.h"
#include "util.h"
#include "networkutil.h"
#include "fileparser.h"
#include "threadpool.h"

using namespace std;

// main to use MGT locally

int main(int argc, char** argv) {
	Timer t;
	t.start();

	if (argc < 6)
	{
		cerr << "Usage: " << argv[0] << " filename maxdeg output mem instances" << endl;
		return 1;
	}

	const char* orig = argv[1];

	string out(orig);
	string parse("-oriented");
	string comb = out + parse;
	const char* base = comb.c_str();

	vx maxDeg = (vx) atoi(argv[2]);


	bool output  = atoi(argv[3]) != 0;

	size_t mem = atoll(argv[4]);

	vx count = (vx) atoi(argv[5]);


	if (maxDeg == 0)
	{
		maxDeg = orient(orig, base, mem, count);
		cout << "Orientation took " << t.lap()<< endl;
		cout << "Max degree " << maxDeg << endl;
	}
	else
	{
	  base = out.c_str();
	}
	Volume info(base, mem,
			maxDeg, count, count, orig);
	info.loadbalance();
	cout << "Load balancing took " << t.lap() << endl;
	ThreadPool calc(output, info);
	cout << "Calculating took " << t.lap() << endl;

	cout << "Triangle num: " << calc.getcount() << endl;

	if (output)
	{
		auto outName = getOutName(base);
		concatenate(outName, info.getsize());
		for (unsigned long i = 0; i < info.getsize(); ++i)
		{
			string name = getName(outName, i);
			remove(name.c_str());
		}
	}

	cout << "Concatenation took " << t.lap() << endl;

	cout << "Total time " << t.total() << endl;
	return 0;
}


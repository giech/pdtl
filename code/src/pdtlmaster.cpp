/*
 * PDTL: Parallel and Distributed Triangle Listing for Massive Graphs
 * Ilias Giechaskiel, George Panagopoulos, Eiko Yoneki
 * 44th International Conference on Parallel Processing (ICPP), Beijing 2015
 * 
 * DOI: 10.1109/ICPP.2015.46
 * 
 * https://github.com/giech/pdtl
 */

#include <stdio.h>
#include <stdlib.h>
#include <signal.h>
#include <errno.h>
#include <atomic>
#include "util.h"
#include "networkutil.h"
#include "fileparser.h"
#include "loadbalance.h"

// Master connects to the various clients and delegates responsibility for
// different sections of the graph

using namespace std;

string adjName;
string degName;
string outName;
vx maxDeg;
vx output;

vx* mems;
vx* instances;

atomic<unsigned long long> triangleCount;

inline int getIndex(int i)
{
	return 6 + 4*i;
}

void makeConnection(const char* ip, 
                    int port, 
                    int serv, 
                    ThreadInfo *info,
		                unsigned start)
{
	struct sockaddr_in sin;
	memset((char*) &sin, 0, sizeof(sin));
	sin.sin_family = AF_INET;
	sin.sin_port = htons(port);
	if (!inet_pton(AF_INET, ip, &sin.sin_addr))
	{
		cerr << ip << " is not a valid IP address" << endl;
		return;
	}

	int soc = socket(PF_INET, SOCK_STREAM, 0);
	if (soc < 0)
	{
		cerr << strerror(errno) << ": Error with setting up socket" << endl;
		return;
	}

	if (connect(soc, (struct sockaddr*) &sin, sizeof(sin)) < 0)
	{
		cerr << strerror(errno) << ": Error with connecting" << endl;
		close(soc);
		return;
	}

	writeVx(soc, maxDeg);
	writeVx(soc, output);
	Timer t;
	t.start();
	writeFile(soc, degName);
	writeFile(soc, adjName);
	cout << "[Server " << serv << "]: Copying files took " << t.lap() << endl;

	vx count = instances[serv];
	writeVx(soc, count);

	vx i;
	unsigned long long total = info->getchunks()[start+count] - info->getchunks()[start];
	for (i = 0;  i < count; ++i)
	{
		unsigned long long begin = info->getchunks()[start+i],
			      end = info->getchunks()[start+i+1];
		writeVx(soc, (vx)((unsigned long long)mems[serv]*count*(end-begin)
					/total));
		writeULL(soc, begin);
		writeULL(soc, end);
		writeULL(soc, *((unsigned long long *)&(info->getavdegree()[start+i])));//FIXME
	}


	unsigned long long tri = readULL(soc);
	cout << "[Server " << serv << "]: Calculating took " << t.lap() << endl;


	if (output)
	{
		string name = getName(outName, serv);
		readFile(soc, name);
	}

	shutdown(soc, SHUT_WR);
	shutdown(soc, SHUT_RD);

	close(soc);
	triangleCount += tri;
}

int main(int argc, char** argv)
{
	if (argc < 10 || argc % 4 != 2)
	{
		cerr << "usage: " << argv[0] << " filename maxdeg memsize instances output ip port mem instances ...." << endl;
		return 1;
	}

	Timer t;
	t.start();

	const char* orig = argv[1];

	string out(orig);
	string parse("-oriented");
	string comb = out + parse;
	const char* base = comb.c_str();
	size_t mymem = atol(argv[3]);
	unsigned mycount = atol(argv[4]);

	maxDeg = (vx) atoi(argv[2]);
	if (maxDeg == 0)
	{
		maxDeg = orient(orig, base, mymem,
				mycount);
		cout << "Orientation took " << t.lap()<< endl;
	}
	adjName = getAdjName(base);
	degName = getDegName(base);
	outName = getOutName(base);

	output = (vx) atoi(argv[5]);

	unsigned servers = (argc - 6)/4;

	mems = new vx[servers];
	instances = new vx[servers];
	unsigned i;

	unsigned totalInstances = 0;

	for (i = 0; i < servers; ++i)
	{
		int index = getIndex(i);
		mems[i] = atoi(argv[index + 2]);
		instances[i] = atoi(argv[index + 3]);
		totalInstances += instances[i];
	}
	totalInstances += mycount;
	thread* threads = new thread[servers];

	triangleCount = 0;

	Volume info(base, mymem, maxDeg, mycount, totalInstances,
			orig);
	info.loadbalance();
	cout << "Load balancing took: " << t.lap() << endl;
	totalInstances = 0;
	for (i = 0; i < servers; ++i)
	{
		int index = getIndex(i);
		const char* ip = argv[index];
		int port = atoi(argv[index + 1]);
		if (port < 0)
		{
			cerr << "Port must be non-negative" << endl;
			return 1;
		}

		threads[i] = thread(makeConnection, ip, port, i, &info, totalInstances);
		totalInstances += instances[i];
	}
	MGTAdjacencyHandler** handlers = new MGTAdjacencyHandler*[mycount];

	unsigned long long total = info.getchunks()[totalInstances+mycount]
		- info.getchunks()[totalInstances];
	thread* mythreads = new thread[mycount];
	unsigned long mytriangles = 0;
	Timer mytimer;
	mytimer.start();
	for (i = 0;  i < mycount; ++i)
	{
		unsigned long long begin = info.getchunks()[totalInstances+i],
			      end = info.getchunks()[totalInstances+i+1];
		const char *name = NULL;
		string s;
		if(output)
		{
			s = getName(getOutName(base), servers);
			name = s.c_str();
		}
		handlers[i] = new MGTAdjacencyHandler(base, 
		                                      maxDeg, 
				                                  mymem*mycount*(end-begin)/total,
				                                  name,
				                                  info.getavdegree()[totalInstances+i]);
		mythreads[i] = thread(&MGTAdjacencyHandler::timedProcessAdjacency, 
		                      handlers[i], 
		                      begin, 
		                      end);
	}

	for(i = 0; i < mycount; i++) {
		mythreads[i].join();
		mytriangles += handlers[i]->getTriangleCount();
		delete handlers[i];
	}
	delete[] handlers;
	delete[] mythreads;
	cout << "[Master]: Calculating took " << mytimer.lap() << endl;

	for (i = 0; i < servers; ++i)
	{
		threads[i].join();
	}

	delete[] threads;
	delete[] mems;
	delete[] instances;


	cout << "Calculating took " << t.lap() << endl;

	if (output)
	{
		concatenate(outName, servers);
	}

	cout << "Concatenation took " << t.lap() << endl;
	triangleCount += mytriangles;
	cout << "Triangle num: " << triangleCount << endl;
	cout << "Total time " << t.total() << endl;
	return 0;
}


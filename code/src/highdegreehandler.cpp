/*
 * PDTL: Parallel and Distributed Triangle Listing for Massive Graphs
 * Ilias Giechaskiel, George Panagopoulos, Eiko Yoneki
 * 44th International Conference on Parallel Processing (ICPP), Beijing 2015
 * 
 * DOI: 10.1109/ICPP.2015.46
 * 
 * https://github.com/giech/pdtl
 */

#include "highdegreehandler.h"

using namespace std;

typedef std::unordered_set<vx> adj_list;

	HighDegreeHandler::HighDegreeHandler(const string input, 
	                                     const string output, 
	                                     vx max, 
	                                     const char* report, 
	                                     unsigned int bs)
: AdjacencyHandler(input, bs)
{
	copyFiles(input, output, bs);

	maxDeg = max;
	adj = new vx[maxDeg];
	neighbors = new vx[maxDeg];

	outName = output;
	tempName = outName + "-temp";

	triangleCount = 0;


	if (report != NULL)
	{
		string outName(report);
		if (outName.size() == 0)
		{
			outName = getOutName(output.c_str());
		}
		out = new FileBuffer(outName, bs);
		intersection = new vx[maxDeg];
	}
	else
	{
		intersection = NULL;
		out = NULL;
	}

	adj_set = NULL; 
}

unsigned long long HighDegreeHandler::getTriangleCount()
{
	return triangleCount;
}

HighDegreeHandler::~HighDegreeHandler()
{
	delete[] adj;
	delete[] neighbors;
	if (out != NULL)
	{
		delete[] intersection;
		out->close();
		delete out;
	}
}


vx HighDegreeHandler::findHighDegVx()
{
	vx u;
	offset = 0;
	for (u = 0; u < graphSize; ++u)
	{
		vx d = deg->getDegree(u);
		if (d > maxDeg)
		{
			cout << "Vertex " << u << " of deg " << d << endl;
			break;
		}

		offset += d;
	}

	return u;
}

void HighDegreeHandler::createHashStructures()
{
	fseek64(fd, offset*sizeof(vx), SEEK_SET);
	adj_set = new adj_list(maxDeg);
	// read and create hashes

	size_t size;

	vx off = 0;
	vx rem = maxDeg;
	while (0 < (size = fread(adj + off, sizeof(vx), rem, fd)))
	{
		rem -= (vx) size;
		off += (vx) size;
	}

	vx i;
	for (i = 0; i < maxDeg; ++i)
	{
		adj_set->insert(adj[i]);
	}

	adj_end = adj_set->end();
}

void HighDegreeHandler::handleSet()
{
	if (containedHigh)
	{
		vx interSize = processIntersection(adj, 
		                                   maxDeg, 
		                                   neighbors, 
		                                   neighborSize,
		                                   intersection);
		vx i;
		for (i = 0; i < interSize; ++i)
		{
			reportTriangle(highDegVx, 
			               lastFrom, 
			               intersection != NULL ? intersection[i] : UNINIT);
		}
	}


	neighborSize = 0;
	containedHigh = false;
}

void HighDegreeHandler::reportTriangle(vx u, vx v, vx w)
{
	++triangleCount;
	if (out != NULL)
	{
		out->addToBuffer(u);
		out->addToBuffer(v);
		out->addToBuffer(w);
	}
}

void HighDegreeHandler::handle()
{
	while ((highDegVx = findHighDegVx()) < graphSize)
	{
		createHashStructures();
		parser = new ParserUtil(tempName); 

		processAdjacency(0, MAX_EDGES);
		handleSet();
		lastFrom = UNINIT;


		parser->close();
		delete parser;
		resetFiles();

		if (adj_set != NULL)
		{
			delete adj_set;
		}
	}
}

void HighDegreeHandler::resetFiles()
{
	delete deg;
	fclose(fd);

	const char* out_str = outName.c_str();
	const char* temp_str = tempName.c_str();

	string adjName = getAdjName(out_str);
	string degName = getDegName(out_str);
	const char* adj_str = adjName.c_str();
	const char* deg_str = degName.c_str();

	remove(adj_str);
	remove(deg_str);


	string tempAdjName = getAdjName(temp_str);
	string tempDegName = getDegName(temp_str);
	rename(tempAdjName.c_str(), adj_str);
	rename(tempDegName.c_str(), deg_str);
	fd = fopen(adj_str, READ_FLAG);

	deg = new DegreeHandler(degName);
}

void HighDegreeHandler::copyFiles(const string from, const string to, vx bufferSize)
{
	vx* buffer = new vx[bufferSize];

	const char* input_str = from.c_str();
	const char* output_str = to.c_str();
	string degName = getDegName(output_str);
	string adjName = getAdjName(output_str);
	copyFile(getDegName(input_str), degName, buffer, bufferSize);
	copyFile(getAdjName(input_str), adjName, buffer, bufferSize);


	delete[] buffer;
}

void HighDegreeHandler::copyFile(const string from, const string to, vx* buffer, vx bufferSize)
{
	cout << from << endl;
	cout << to << endl;
	FILE* rd = fopen(from.c_str(), READ_FLAG);
	FILE* wr = fopen(to.c_str(), WRITE_FLAG);

	size_t size;
	while (0 < (size = fread(buffer, sizeof(vx), bufferSize, rd)))
	{
		vx total = 0;
		while (size > 0)
		{
			vx written = (vx) fwrite(buffer + total, sizeof(vx), size, wr);
			size -= written;
			total += written;
		}
	}

	fclose(rd);
	fclose(wr);
}

void HighDegreeHandler::overallSetUp()
{
	neighborSize = 0;
	lastFrom = UNINIT;
	containedHigh = false;
}

void HighDegreeHandler::processPhase()
{

}

void HighDegreeHandler::phaseSetUp()
{

}

bool HighDegreeHandler::handleEdge(vx from, vx to, vx fromDeg)
{
	bool contains_from = adj_set->find(from) != adj_end;
	bool contains_to = adj_set->find(to) != adj_end;
	bool high_from = (from == highDegVx);
	bool high_to = (to == highDegVx);

	if ((high_from && contains_to) || (high_to && contains_from))
	{
		return true;	
	}


	parser->addEdge(from, to);

	if (high_from)
	{
		return true;
	}

	if (contains_from && contains_to)
	{
		// only report 1 dir
		if (from < to)
		{
			reportTriangle(highDegVx, from, to);
		}
	}
	else
	{
		if (from != lastFrom)
		{
			handleSet();
			lastFrom = from;
		}

		if (high_to)
		{
			containedHigh = true;
		}

		if (contains_to)
		{
			neighbors[neighborSize] = to;
			++neighborSize;
		}
	}

	return true;
}

void HighDegreeHandler::overallTearDown()
{

}

int main(int argc, char* argv[])
{
	Timer t;
	t.start();
	if (argc != 5)
	{
		cerr << "Usage: " << argv[0] << " input output maxdeg report" << endl;
		exit(1);
	}

	string input = string(argv[1]);
	string output = string(argv[2]);
	vx maxDeg = (vx) atoi(argv[3]);
	const char* report = atoi(argv[4]) != 0 ? "" : NULL;

	HighDegreeHandler handler(input, output, maxDeg, report);
	cout << "Initialized handler in " << t.lap() << endl;
	handler.handle();
	cout << "Processed high degrees in " << t.lap() << endl;
	cout << "Degrees had " <<  handler.getTriangleCount() << " triangles" << endl;
	return 0;
}


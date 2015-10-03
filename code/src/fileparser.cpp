/*
 * PDTL: Parallel and Distributed Triangle Listing for Massive Graphs
 * Ilias Giechaskiel, George Panagopoulos, Eiko Yoneki
 * 44th International Conference on Parallel Processing (ICPP), Beijing 2015
 * 
 * DOI: 10.1109/ICPP.2015.46
 * 
 * https://github.com/giech/pdtl
 */

#include <sstream>
#include <iostream>
#include <fstream>
#include <set>
#include <thread>
#include <cassert>
#include <fcntl.h>
#include <unistd.h>
#include <sys/types.h>
#include <sys/mman.h>

#include "fileparser.h"
#include "util.h"
#include "parserutil.h"
#include "degreehandler.h"
#include "adjacencyhandler.h"

using namespace std;

typedef set<vx> adj_list; // important that this ordered set

class UndirectAdjacencyHandler : public AdjacencyHandler {
	public:
		UndirectAdjacencyHandler(const string input, const string output)
			: AdjacencyHandler(input)
		{
			parser = new ParserUtil(output);
		}

		~UndirectAdjacencyHandler()
		{
			delete parser;
		}

		vx getMaxDegree()
		{
			return maxDeg;
		}

	private:
		ParserUtil* parser;
		adj_list* lists;
		vx maxDeg;

		virtual void overallSetUp()
		{
			lists = new adj_list[graphSize];
		}

		virtual void processPhase()
		{
			vx i;
			for (i = 0; i < graphSize; ++i)
			{
				adj_list list = lists[i];
				adj_list::iterator iter;
				adj_list::iterator end = list.end();
				for (iter = list.begin(); iter != end; ++iter)
				{
					parser->addEdge(i, *iter);
				}
			}
			parser->close();
			maxDeg = parser->getMaxDegree();
		}

		virtual void phaseSetUp(){}

		virtual bool handleEdge(vx from, vx to, vx fromDeg)
		{
			lists[from].insert(to);
			lists[to].insert(from);
			return true;
		}

		virtual void overallTearDown()
		{
			delete[] lists;
		}
};

vx undirect(const char* input, const char* output)
{
	UndirectAdjacencyHandler handler(input, output);
	handler.processAdjacency(0, MAX_EDGES);
	return handler.getMaxDegree();
}


class OrienterAdjacencyHandler : public AdjacencyHandler {
	public:
		OrienterAdjacencyHandler(const string input, 
				                     const string output, 
				                     size_t bufferSize = DEFAULT_BUF, 
				                     bool fillstart = true, 
				                     bool fillend = true)
			: AdjacencyHandler(input, bufferSize)
		{
			parser = new ParserUtil(output, bufferSize, fillstart, fillend);
		}

		OrienterAdjacencyHandler(const string input, 
				                     const string output, 
				                     DegreeHandler* deg, 
				                     size_t bufferSize = DEFAULT_BUF, 
				                     bool fillstart = true, 
				                     bool fillend = true)
			: AdjacencyHandler(input, deg, bufferSize)
		{
			parser = new ParserUtil(output, bufferSize, fillstart, fillend);
		}

		~OrienterAdjacencyHandler()
		{
			delete parser;
		}

		vx getMaxDegree()
		{
			return maxDeg;
		}

	private:
		ParserUtil* parser;
		vx maxDeg;

		virtual void overallSetUp(){}
		virtual void processPhase(){}

		virtual void phaseSetUp(){}
		virtual bool handleEdge(vx from, vx to, vx degree)
		{
			vx degFrom = degree;
			vx degTo = deg->getDegree(to);

			if ((degFrom < degTo) || (degFrom == degTo && from < to))
			{
				parser->addEdge(from, to);
			}
			return true;
		}

		virtual void overallTearDown()
		{
			maxDeg = parser->getMaxDegree();
			parser->close();
		}
};

vx orient(const char* input, const char* output, size_t degMB, unsigned threads)
{
	string degFile = getDegName(input);
	DegreeHandler **randeg = new DegreeHandler *[threads];
	DegreeHandler *deg = NULL;
	bool random;
	size_t degBufferSize = degMB*MB_TO_B*threads;
	size_t size = getFileSize(degFile.c_str())/sizeof(vx);
	if(degBufferSize == 0)
		degBufferSize = size;
	if (size <= degBufferSize)
	{
		random = false;
		degBufferSize = size;
		cout << "Custom degree handler of size " << degBufferSize << endl;
		deg = new DegreeHandler(degFile, degBufferSize);
		cout << "Deg handler initialized" << endl;
	} else {
		random = true;
		cout << "Random access degree handler" << endl;
	}
	if(threads == 1)
	{
		if(random)
			deg = new NonSequentialDegreeHandler(degFile);
		OrienterAdjacencyHandler handler(input, output, deg);
		handler.processAdjacency(0, MAX_EDGES);
		delete deg;
		delete [] randeg;
		return handler.getMaxDegree();
	}
	size_t adjsize = getFileSize(getAdjName(input).c_str())/sizeof(vx);
	OrienterAdjacencyHandler **handlers = new OrienterAdjacencyHandler*[threads];
	string filenames[threads];
	thread *threadarr = new thread[threads];
	for(unsigned i = 0; i < threads; i++)
	{
		ostringstream tempname;
		tempname << output << "-" << i;
		filenames[i] = tempname.str();
		if(random)
		{
			randeg[i] = new NonSequentialDegreeHandler(degFile);
			handlers[i] = new OrienterAdjacencyHandler(input,
					filenames[i].c_str(),
					randeg[i],
					DEFAULT_BUF,
					i == 0 ? true : false,
					i == threads-1 ? true : false);
		}
		else
		{
			handlers[i] = new OrienterAdjacencyHandler(input,
					filenames[i].c_str(),
					deg,
					DEFAULT_BUF,
					i == 0 ? true : false,
					i == threads-1 ? true : false);
		}
		threadarr[i] = thread(&OrienterAdjacencyHandler::processAdjacency,
				handlers[i], adjsize/threads*i,
				i == threads-1 ? adjsize : adjsize/threads*(i+1));
	}
	for(unsigned i = 0; i < threads; i++)
	{
		threadarr[i].join();
		if(random)
			delete randeg[i];
	}
	delete[] randeg;
	if(!random && deg != NULL)
		delete deg;
	vx maxDeg = 0;
	for(unsigned i = 0; i < threads; i++)
	{
		vx Deg = handlers[i]->getMaxDegree();
		if(Deg > maxDeg)
			maxDeg = Deg;
		delete handlers[i];
	}
	delete[] handlers;
	FILE *files[threads];
	FILE *outputfd;
	size_t buffSize = degMB*MB_TO_B;
	if(buffSize == 0)
		buffSize = DEFAULT_BUF;
	vx *buffer = new vx[buffSize];
	outputfd = fopen(getAdjName(output).c_str(), "w");
	for(unsigned i = 0; i < threads; i++) // append files to create .adj file
	{
		const char *name = getAdjName(filenames[i].c_str()).c_str();
		files[i] = fopen(name, "r");
		size_t size;
		while(0 < (size = fread(buffer, sizeof(vx), buffSize, files[i])))
		{
			fwrite(buffer, sizeof(vx), size, outputfd);
		}
		fclose(files[i]);
		remove(name);
	}
	fclose(outputfd);
	// For the deg file, we need to check that the last line of the
	// previous thread's file is not the same vertex as our first line;
	// if it is then add them.
	outputfd = fopen(getDegName(output).c_str(), "w");
	vx next[2];
	vx previous[2];
	string degnames[threads];
	degnames[0] = getDegName(filenames[0].c_str());
	files[0] = fopen(degnames[0].c_str(), "r");
	for(unsigned i = 0; i < threads; i++)
	{
		size_t filesize = getFileSize(degnames[i].c_str())/sizeof(vx);
		if(!filesize) goto del;
		size_t rem, size;
		rem = filesize - ftell(files[i])/sizeof(vx) - 2;
		while(0 < (size = fread(buffer, sizeof(vx), min(buffSize, rem),
						files[i])))
		{
			fwrite(buffer, sizeof(vx), size, outputfd);
			rem -= size;
		}
		assert(2 == fread(previous, sizeof(vx), 2, files[i]));
		if(i != threads-1)
		{
tryagain:
			degnames[i+1] = getDegName(filenames[i+1].c_str());
			files[i+1] = fopen(degnames[i+1].c_str(), "r");
			if(!fread(next, sizeof(vx), 2, files[i+1])) // empty file
			{
				if(i+1 == threads-1) { // no other .deg file left
					fwrite(previous, sizeof(vx), 2, outputfd);
					goto del;
				}
				fclose(files[i]); // else delete our file, increase i and try again
				remove(degnames[i].c_str());
				i++;
				goto tryagain;
			}
			if(next[0] == previous[0]) {
				next[1] += previous[1];
				if(next[1] > maxDeg)
					maxDeg = next[1]; // parser would not have noticed this
				// because it split up in the threads
			} else {
				unsigned long index = 0;
				buffer[index++] = previous[0];
				buffer[index++] = previous[1];
				previous[0]++;
				// insert possible missing vertices with no edges
				while(previous[0] < next[0])
				{
					if(index == buffSize)
					{
						fwrite(buffer, sizeof(vx), index, outputfd);
						index = 0;
					}
					buffer[index++] = previous[0];
					buffer[index++] = 0;
					previous[0]++;
				}
				fwrite(buffer, sizeof(vx), index, outputfd);
			}
			fwrite(next, sizeof(vx), 2, outputfd);
		} else
			fwrite(previous, sizeof(vx), 2, outputfd);

del:
		fclose(files[i]);
		remove(degnames[i].c_str());
	}
	fclose(outputfd);
	delete[] buffer;
	return maxDeg;
}


class OrderedAdjacencyHandler : public AdjacencyHandler {
	public:
		OrderedAdjacencyHandler(const string input, 
		                        const string output, 
		                        size_t bufferSize = DEFAULT_BUF)
			: AdjacencyHandler(input, bufferSize)
		{
			parser = new ParserUtil(output, bufferSize);
		}

		~OrderedAdjacencyHandler()
		{
			delete parser;
		}

	private:
		ParserUtil* parser;
		vx lastFrom;
		adj_list current;

		virtual void overallSetUp(){}
		virtual void processPhase(){}

		virtual void phaseSetUp()
		{
			lastFrom = UNINIT;
		}

		void printCurrent()
		{
			adj_list::iterator iter;
			adj_list::iterator end = current.end();
			for (iter = current.begin(); iter != end; ++iter)
			{
				parser->addEdge(lastFrom, *iter);
			}
			current.clear();
		}

		virtual bool handleEdge(vx from, vx to, vx degree)
		{
			if (lastFrom != from)
			{
				printCurrent();
				lastFrom = from;
			}

			current.insert(to);
			return true;
		}

		virtual void overallTearDown()
		{
			printCurrent();
			parser->close();
		}
};

void orderNeighbors(const char* input, const char* output)
{
	OrderedAdjacencyHandler handler(input, output);
	handler.processAdjacency(0, MAX_EDGES);
}


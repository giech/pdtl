/*
 * PDTL: Parallel and Distributed Triangle Listing for Massive Graphs
 * Ilias Giechaskiel, George Panagopoulos, Eiko Yoneki
 * 44th International Conference on Parallel Processing (ICPP), Beijing 2015
 * 
 * DOI: 10.1109/ICPP.2015.46
 * 
 * https://github.com/giech/pdtl
 */

#include <cstdio>
#include <vector>
#include <iostream>
#include <sstream>
#include <cassert>
#include <fcntl.h>
#include <fstream>
#include <unistd.h>
#include <cmath>
#include <sys/resource.h>
#include <sys/types.h>
#include <sys/mman.h>

#include "fileconverter.h"
#include "util.h"
#include "parserutil.h"
#include "adjacencyhandler.h"

using namespace std;

inline int todigit(char input)
{
	return input - '0';
}

void parseAdjacencyList(const char* input, const char* output, int starter)
{
	int fd = open(input, O_RDONLY);
	ParserUtil parser(output);
	size_t size = lseek(fd, 0, SEEK_END);
	lseek(fd, 0, SEEK_SET);
	char *buf = static_cast<char *> (mmap(nullptr, size, PROT_READ, MAP_PRIVATE, fd, 0));
	vx from, to;
	bool foundfrom = false;
	for(size_t i = 0; i < size; i++)
	{
		if(buf[i] == '#')
		{
			while(i < size && buf[++i] != '\n');
			++i;
			if(i >= size) break;
		}
		while(!isdigit(buf[i]) && i < size)
			i++;
		if(i == size) break;
		if(!foundfrom)
		{
			from = todigit(buf[i]);
			int temp = todigit(buf[++i]);
			while(temp <= 9 && temp >= 0)
			{
				from = from*10 + temp;
				temp = todigit(buf[++i]);
			}
			foundfrom = true;
		} else {
			to = todigit(buf[i]);
			int temp = todigit(buf[++i]);
			while(temp <= 9 && temp >= 0)
			{
				to = to*10 + temp;
				temp = todigit(buf[++i]);
			}
			foundfrom = false;
			parser.addEdge(from, to);
		}
	}
	munmap(buf, size);
	close(fd);
}



int xstream_comp(const void *first, const void *second)
{
	const vx *firsta = (vx *) first;
	const vx *seconda = (vx *) second;
	if(firsta[0] < seconda[0]) return -1;
	else if(firsta[0] == seconda[0]) {
		if(firsta[1] < seconda[1])
			return -1;
		else if(firsta[1] == seconda[1])
			return 0;
		else
			return 1;
	}
	else
		return 1;
}

// qsort chunks, save them in files and then merge
void parseXStream(const char *inputname, 
		              const char *outputname, 
		              size_t mem, 
		              unsigned int vn)
{
	FILE *input;
	input = fopen(inputname, "r");
	struct rlimit filelim;
	getrlimit(RLIMIT_NOFILE, &filelim);
	filelim.rlim_cur = filelim.rlim_max;
	setrlimit(RLIMIT_NOFILE, &filelim);
	if(mem == 0)
	{
		fseek64(input, 0, SEEK_END);
		mem = ftell(input) + 8; // size of file plus overhead
		fseek64(input, 0, SEEK_SET);
	}
	else
	{
	  mem *= MB_TO_B;
	}

	size_t bufSize = ((size_t)(mem/sizeof(vx)/vn)) * vn;
	vx *buffer = (vx*)malloc(bufSize*sizeof(vx));
	size_t size, index = 0;
	vector <FILE *> temp_files;
	while(0 < (size = fread(buffer, sizeof(vx), bufSize, input)))
	{
		if(size % vn)
		{
			cerr << "Error: Corrupted file " << inputname << endl;
			fclose(input);
			exit(1);
		}
		qsort(buffer, size/vn, sizeof(vx) * vn, &xstream_comp);
		FILE *temp;
		ostringstream tempname;
		tempname << "__temp_" << index;
		temp = fopen(tempname.str().c_str(), "w+");
		if(temp == NULL) { cerr << errno << endl; return; }
		fwrite(buffer, sizeof(vx), size, temp);
		fflush(temp);
		index++;
		temp_files.push_back(temp);
	}

	// merge the files
	size_t size_read[index], indices[index];
	bool complete[index];
	size_t chunk = ((size_t) (bufSize / (index+1) / vn)) * vn;
	if(chunk == 0)
	{
		cerr << "Too little memory" << endl;
		fclose(input);
		exit(1);
	}
	buffer = (vx *) realloc(buffer, chunk*index*sizeof(vx));
	ParserUtil parser(outputname, chunk/2);
	for(vx i = 0; i < index; i++) {
		fseek64(temp_files[i], 0, SEEK_SET);
		size_read[i] = fread(&buffer[chunk*i], sizeof(vx), chunk,
				temp_files[i]);
		indices[i] = 0;
		complete[i] = false;
	}
	vx index_smallest = 0, i;
	vx previous[2];
	bool has_previous = false;
	while(true)
	{
		for(i = 0; i < index; i++)
			if(!complete[i]) {
				index_smallest = i;
				break;
			}
		if(i == index)
			break;
		// find the smallest
		for(i++; i < index; i++)
		{
			if(complete[i]) continue;
			if(xstream_comp(&buffer[chunk*index_smallest + indices[index_smallest]],
						&buffer[chunk*i + indices[i]]) > 0)
				index_smallest = i;
		}
		unsigned long location = chunk*index_smallest + indices[index_smallest];
		if((!has_previous || 
		    buffer[location] != previous[0] || 
				buffer[location+1] != previous[1]) &&
				buffer[location] != buffer[location+1])
		{
			has_previous = true;
			previous[0] = buffer[location];
			previous[1] = buffer[location+1];
			parser.addEdge(previous[0], previous[1]);
		}
		indices[index_smallest] += vn;
		if(indices[index_smallest] == size_read[index_smallest]) {
			size_read[index_smallest] = fread(&buffer[chunk*index_smallest], 
			                                  sizeof(vx), 
			                                  chunk,
					                              temp_files[index_smallest]);
			if(size_read[index_smallest] <= 0)
			{
				complete[index_smallest] = true;
				fclose(temp_files[index_smallest]);
				ostringstream tempname;
				tempname << "__temp_" << index_smallest;
				unlink(tempname.str().c_str());
			}
			indices[index_smallest] = 0;
		}
	}
	parser.close();
	fclose(input);
	free(buffer);
}



class OPTConverter : public AdjacencyHandler {
	public:
		OPTConverter(const char *input, 
		             const char *output, 
		             size_t bufferSize = DEFAULT_BUF)
			: AdjacencyHandler(input, bufferSize), buf_size(bufferSize)
		{
			degree_name = getDegName(input);
			size = getFileSize(degree_name.c_str())/sizeof(vx);
			buf = new vx[size];
			inds = new vx[size/2];
			start = new unsigned long long[size/2];
			cur = new unsigned long long[size/2];
			cout << "Reading .deg file" << endl;
			fdin = fopen(degree_name.c_str(), READ_FLAG);
			if(fread(buf, sizeof(vx), size, fdin) != size)
			{
				cerr << "Error reading" << endl;
				exit(1);
			}
			cout << "Passing .adj file for A-A edges" << endl;
			int fd = open(getAdjName(input).c_str(), O_RDONLY);
			size_t adjsize = lseek(fd, 0, SEEK_END);
			lseek(fd, 0, SEEK_SET);
			vx *adjbuf = static_cast<vx *> (mmap(nullptr, 
			                                     adjsize, 
			                                     PROT_READ, 
			                                     MAP_PRIVATE, 
			                                     fd, 
			                                     0));
			unsigned long long counter = 0, deg = buf[1];
			vx ver = 0;
			for(size_t i = 0; i < adjsize/sizeof(vx); i++)
			{
				while(!deg)
				{
					ver++;
					deg = buf[2*ver+1];
				}
				if(adjbuf[i] == ver)
				{
					counter++;
					buf[ver*2+1]--;
				}
				deg--;
			}
			if(deg)
			{
				cout << "Something is wrong, deg != 0" << endl;
			}
			munmap(adjbuf, adjsize);
			close(fd);
			cout << counter << " edges removed" << endl;
			cout << "Sorting" << endl;
			qsort(buf, size/2, sizeof(vx)*2, &(OPTConverter::compardeg));
			cout << "Indexing" << endl;
			num_ver = size/2;
			unsigned long long cumu = 0;
			for(size_t i = 0; i < size; i+=2)
			{
				start[i/2] = cur[i/2] = cumu;
				cumu += buf[i+1];
				inds[buf[i]] = i/2;
				if(!buf[i+1])
				{
					num_ver = i/2;
					break;
				}
			}
			delete[] buf;
			edges = new vx[getFileSize(getAdjName(input).c_str())/sizeof(vx)];
			edge_size = 0;
			fdout = fopen(output, "w");
			fprintf(fdout, "%u ", num_ver);
		}

	private:
		FILE *fdin, *fdout;
		vx num_ver;
		size_t size, edge_size, buf_size;
		vx *buf, *inds, *edges;
		char *buffer;
		unsigned long long *start, *cur;
		string degree_name;

		static int compardeg(const void *first, const void *second)
		{
			vx degf = static_cast<const vx *> (first)[1];
			vx degs = static_cast<const vx *> (second)[1];
			if(degf > degs)
				return -1;
			else if(degf < degs)
				return 1;
			else
				return 0;
		}

		static int comparedge(const void *first, const void *second)
		{
			vx f = *static_cast<const vx *>(first);
			vx s = *static_cast<const vx *>(second);
			if(f < s)
				return -1;
			else if(f > s)
				return 1;
			else
				return 0;
		}

		virtual void overallSetUp() { cout << "Reading .adj" << endl; }
		virtual void processPhase() {}
		virtual void phaseSetUp() {}
		virtual void overallTearDown()
		{
			cout << "Writing final graph" << endl;
			delete[] inds;
			size_t output_index = 0;
			buffer = new char[buf_size];
			fprintf(fdout, "%lu\r\n", edge_size/2);
			for(vx i = 0; i < num_ver; i++)
			{
				if(i != num_ver-1 && cur[i] != start[i+1])
					cout << "Something is wrong cur != start for " << i << endl;
				qsort(&edges[start[i]], 
				      cur[i]-start[i], 
				      sizeof(vx), 
				      &(OPTConverter::comparedge));
				for(unsigned long long j = start[i]; j < cur[i]; j++) {
					if(i < edges[j]) break;
					long long left = buf_size - output_index;
					int written = snprintf(&buffer[output_index], left,
							"%u %u\r\n", i, edges[j]);
					if(written >= left)
					{
						fwrite(buffer, sizeof(char), output_index, fdout);
						output_index = sprintf(buffer, "%u %u\r\n", i, edges[j]);
					} else
						output_index += written;
				}
			}
			delete[] start;
			delete[] cur;
			delete[] edges;
			fwrite(buffer, sizeof(char), output_index, fdout);
			delete[] buffer;
			fclose(fdout);
		}
		virtual bool handleEdge(vx from, vx to, vx degree)
		{
			if(from != to) {
				edge_size++;
				edges[cur[inds[from]]++] = inds[to];
			}
			return true;
		}
};

void convertToOPT(const char* input, const char* output)
{
	OPTConverter converter(input, output);
	converter.processAdjacency(0, MAX_EDGES);
}

class XStreamConverter : public AdjacencyHandler {
	public:
		XStreamConverter(const char* input, 
		                 const char *output, 
		                 size_t bufferSize = DEFAULT_BUF)
			: AdjacencyHandler(input, bufferSize), buf(output, bufferSize)
		{}

	private:
		FileBuffer buf;
		virtual void overallSetUp(){}
		virtual void processPhase(){}

		virtual void phaseSetUp(){}
		virtual bool handleEdge(vx from, vx to, vx degree)
		{
			buf.addToBuffer(from);
			buf.addToBuffer(to);
			return true;
		}

		virtual void overallTearDown()
		{}
};

void convertToXStream(const char* input, const char* output)
{
	XStreamConverter converter(input, output);
	converter.processAdjacency(0, MAX_EDGES);
}


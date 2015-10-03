/*
 * PDTL: Parallel and Distributed Triangle Listing for Massive Graphs
 * Ilias Giechaskiel, George Panagopoulos, Eiko Yoneki
 * 44th International Conference on Parallel Processing (ICPP), Beijing 2015
 * 
 * DOI: 10.1109/ICPP.2015.46
 * 
 * https://github.com/giech/pdtl
 */

#include "filebuffer.h"

using namespace std;

// TODO: check not closed

FileBuffer::FileBuffer(const string file, size_t size)
{
	out = fopen(file.c_str(), WRITE_FLAG);
	bufferSize = size;
	buffer = new vx[bufferSize];
	bufferIndex = 0;
	closed = false;
}

FileBuffer::~FileBuffer()
{
	if (!closed)
	{
		close();
	}

	delete[] buffer;
}

void FileBuffer::addToBuffer(vx v)
{
	buffer[bufferIndex] = v;
	++bufferIndex;
	if (bufferIndex == bufferSize)
	{
		flush();
	}
}

void FileBuffer::addToBuffer(vx* v, size_t size)
{
	size_t i;
	for (i = 0; i < size; ++i)
	{
		addToBuffer(v[i]);
	}
}

void FileBuffer::flush()
{
	size_t total = 0;
	while (bufferIndex > 0)
	{
		size_t written = fwrite(buffer + total, sizeof(vx), bufferIndex, out);
		bufferIndex -= written;
		total += written;
	}
}

void FileBuffer::close()
{
	closed = true;
	flush();
	fclose(out);
}


/*
 * PDTL: Parallel and Distributed Triangle Listing for Massive Graphs
 * Ilias Giechaskiel, George Panagopoulos, Eiko Yoneki
 * 44th International Conference on Parallel Processing (ICPP), Beijing 2015
 * 
 * DOI: 10.1109/ICPP.2015.46
 * 
 * https://github.com/giech/pdtl
 */

#include "networkutil.h"
#include "filebuffer.h"
#include <fstream>

#define RECV_FLAG MSG_WAITALL
#define SEND_FLAG 0

#define SHIFT 32
#define MASK 0xFFFFFFFF


const unsigned long long BUFFER_SIZE = DEFAULT_BUF; //2048;
using namespace std;

// Since the following 2 functions are not standard, we used
// http://revoman.tistory.com/entry/Implementation-of-htonll-ntohll-uint64t-byte-ordering
unsigned long long fromNetworkULL(unsigned long long u)
{
	int x = 1;

	/* little endian */
	if(*(char *)&x == 1)
		return ((((uint64_t)ntohl((u_long) (u & MASK))) << SHIFT) + ntohl((u_long) ((u >> SHIFT) & MASK)));

	/* big endian */
	else
		return u;
}

unsigned long long toNetworkULL(unsigned long long u)
{
	int x = 1;

	/* little endian */
	if(*(char *)&x == 1)
		return ((((uint64_t)htonl((u_long) (u & MASK))) << SHIFT) + htonl((u_long) ((u >> SHIFT) & MASK)));

	/* big endian */
	else
		return u;
}

vx fromNetwork(vx u)
{
#ifdef BIT64
	return fromNetworkULL(u);
#else
	return ntohl(u);
#endif
}

vx toNetwork(vx u)
{
#ifdef BIT64
	return toNetworkULL(u);
#else
	return htonl(u);
#endif
}


vx readVx(int socket)
{
	vx ans;

	if (recv(socket, (char*) &ans, sizeof(ans), RECV_FLAG) != sizeof(ans))
	{
		cout << "ERROR RECEIVING" << endl;
		return 0; // TODO: error
	}

	vx temp =  fromNetwork(ans);
	cout << "READ " << temp << endl; 
	return temp;
}

void writeVx(int socket, vx v)
{
	vx temp = toNetwork(v);

	cout << "WRITING " << v << endl;

	if (send(socket, (char*) &temp, sizeof(temp), SEND_FLAG) != sizeof(temp))
	{
		// TODO: error
		cout << "ERROR SENDING" << endl;
	}
}

unsigned long long readULL(int sock)
{
	unsigned long long tri;
	if (recv(sock, (char*) &tri, sizeof(tri), RECV_FLAG) != sizeof(tri))
	{
		cout << "Error: " << errno << endl;
		return 0; // TODO: error
	}

	unsigned long long temp = fromNetworkULL(tri);
	cout << "READ " << temp << endl;
	return temp;
}

void writeULL(int sock, unsigned long long tri)
{
	cout << "WRITING " << tri << endl;
	unsigned long long temp = toNetworkULL(tri);
	if (send(sock, (char*) &temp, sizeof(temp), SEND_FLAG) != sizeof(temp))
	{
		cout << "Error: " << errno << endl;
		// TODO error
	}
}

void readFile(int socket, string out)
{
	vx* buf = new vx[BUFFER_SIZE]; /* Temporary buffer */

	int length;
	unsigned long long fileSize = readULL(socket);
	unsigned long long remaining = fileSize;

	FileBuffer b(out);
	while ((length = recv(socket, (char*) buf, (int) (min(BUFFER_SIZE, remaining)*sizeof(vx)), RECV_FLAG)) > 0)
	{
		/*
		   int remainder = length %sizeof(vx);
		   while (remainder != 0)
		   {
		   int r = recv(socket, ((char*) buf) + length, 4 - remainder, 0);
		   if (r <= 0)
		   {
		   cout << "RECV ERROR: " << r << endl;
		   cout << "PROBLEM!!!" << endl;
		   }
		   length += r;
		   remainder -= r;
		   }
		 */

		if (length % sizeof(vx) != 0)
		{
			cout << "PROBLEM WITH FLAG" << endl;
		}

		vx size = (vx) length/sizeof(vx);
		remaining -= size;
		b.addToBuffer(buf, size);
	}

	b.close();
	delete[] buf;
}

void writeFile(int sock, std::string in)
{
	const char* in_str = in.c_str();
	FILE* fd = fopen(in_str, READ_FLAG);
	vx* buf = new vx[BUFFER_SIZE];
	unsigned long long filesize = getFileSize(in_str);
	writeULL(sock, filesize/sizeof(vx));

	size_t size;
	while (0 < (size = fread(buf, sizeof(vx), BUFFER_SIZE, fd)))
	{
		vx total = 0;
		int sendSize = (int) size*sizeof(vx);

		while (sendSize > 0)
		{
			int written = send(sock, ((char*) buf) + total, sendSize, SEND_FLAG);
			if (written <= 0)
			{
				//todo something
				cout << "SEND ERROR " << written << endl;
				cout << "PROBLEM!" << endl;
			}

			sendSize -= written;
			total += written;
		}
	}

	fclose(fd);
	delete[] buf;
}

void concatenate(string baseName, int count)
{
	const char* out_str = baseName.c_str();
	ofstream out(out_str, ios_base::binary);
	int i;
	for (i = 0; i < count; ++i)
	{
		string ind = getName(baseName, i);

		ifstream in(ind, ios_base::binary);
		if (in.peek() != EOF)
		{
			out << in.rdbuf();
		}
		in.close();
	}
	out.close();
}

std::string getName(string baseName, int i)
{
	string s = to_string(i);
	return baseName + "-" + s;
}


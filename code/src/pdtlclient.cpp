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
#include <cassert>
#include "util.h"
#include "networkutil.h"
#include "mgt.h"

#define MAX_PENDING 10000

// Responsible for listening to connections from master, and calculating
// the triangles for the appropriate sections

int socket_number; /* Socket descriptor of listening connection */

using namespace std;

string base_out;

bool del;

/* used to exit cleanly */
void catch_sigint(int sig)
{
	cerr << "Caught signal " << sig << ". Shutting down server" << endl;
	close(socket_number);
	exit(128 + sig);
}

/* Function to handle connection. Modular to think about threads in long term */
/* port is in host architecture */
void handle_connection(int sock, int number)
{
	string base = base_out + "-" + to_string(number);
	const char* base_str = base.c_str();
	string degName = getDegName(base_str);
	const char* degName_str = degName.c_str();
	string adjName = getAdjName(base_str);
	const char* adjName_str = adjName.c_str();
	string outName = getOutName(base_str);
	const char* outName_str = outName.c_str();

	vx maxDeg = readVx(sock);
	vx output = readVx(sock); // really a boolean
	bool out = output != 0;
	readFile(sock, degName_str);
	readFile(sock, adjName_str);

	vx count = readVx(sock); // how many
	unsigned long long* los = new unsigned long long[count];
	unsigned long long* his = new unsigned long long[count];
	MGTAdjacencyHandler** handlers = new MGTAdjacencyHandler*[count];

	vx i;

	for (i = 0; i < count; ++i)
	{
		vx mem = readVx(sock);
		unsigned long long avdegree;
		los[i] = readULL(sock);
		his[i] = readULL(sock);
		avdegree = readULL(sock);
		void *point = &avdegree;
		const char* name = NULL;
		string s;
		if (out)
		{
			s = getName(outName, i);
			name = s.c_str();
		}

		handlers[i] = new MGTAdjacencyHandler(base, 
		                                      maxDeg, 
		                                      mem, 
		                                      name,
				                                  *((double *)point)); //FIXME
	}


	thread* threads = new thread[count];

	if(system("sync ; sysctl vm.drop_caches=3")); // clear caches

	for (i = 0; i < count; ++i)
	{
		threads[i] = thread(&MGTAdjacencyHandler::timedProcessAdjacency, 
		                    handlers[i], 
		                    los[i], 
		                    his[i]);
	}


	for (i = 0; i < count; ++i)
	{
		threads[i].join();
	}

	unsigned long long triangleCount = 0;
	for (i = 0; i < count; ++i)
	{
		unsigned long long tri = handlers[i]->getTriangleCount();
		triangleCount += tri;
		delete handlers[i];
	}

	cout << "Total count: " << triangleCount << endl;

	writeULL(sock, triangleCount);	

	if (out)
	{
		concatenate(base, count);
		writeFile(sock, outName_str);
	}

	shutdown(sock, SHUT_WR);
	shutdown(sock, SHUT_RD);

	close(sock);

	delete[] los;
	delete[] his;
	delete[] handlers;
	delete[] threads;

	if (del)
	{
		remove(degName_str);
		remove(adjName_str);
		if (out)
		{
			remove(outName_str);
			for (i = 0; i < count; ++i)
			{
				string name = getName(outName, i);
				remove(name.c_str());
			}
		}
	}

	cout << "Finished" << endl;
}

int main(int argc, char** argv)
{
	struct sockaddr_in sin;
	int port;
	int new_socket; /* Socket descriptor of new connection */
	socklen_t len = sizeof(struct sockaddr_in);

	if (argc != 3)
	{
		cerr << "Usage: " << argv[0] << " port delete" << endl;
		exit(1);
	}

	port = atoi(argv[1]);

	if (port < 0)
	{
		cerr << "Port must be non-negative" << endl;
		exit(1);
	}

	base_out = string(argv[1]);

	del = atoi(argv[2]) != 0;
	cout << "Will" << (del ? " " : " not ") << "delete files" << endl;

	memset((char*) &sin, 0, sizeof(sin));
	sin.sin_family = AF_INET;
	sin.sin_addr.s_addr = INADDR_ANY;
	sin.sin_port = htons(port); /* Network protocol! */


	socket_number = (int) socket(PF_INET, SOCK_STREAM, 0);

	if (socket_number < 0)
	{
		cerr << strerror(errno) << ": Error with setting up socket" << endl;
		exit(1);
	}

	if (::bind(socket_number, (struct sockaddr*) &sin, sizeof(sin)) < 0)
	{
		cerr << strerror(errno) << ": Error with binding socket on " << port << endl;
		exit(1);
	}

	listen(socket_number, MAX_PENDING);
	signal(SIGINT, catch_sigint); /* Register to exit cleanly */

	cerr << "Listening for up to " << MAX_PENDING << " connections on " << port << endl;

	int connections = 0;

	while (true)
	{
		new_socket = (int) accept(socket_number, (struct sockaddr*) &sin, &len);
		if (new_socket < 0)
		{
			cerr << strerror(errno) << ": Error accepting connection. Shutting down server" << endl;
			close(socket_number);
			exit(1);
		}

		++connections;

		handle_connection(new_socket, connections);
	}

	exit(0);
}


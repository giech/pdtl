/*
 * PDTL: Parallel and Distributed Triangle Listing for Massive Graphs
 * Ilias Giechaskiel, George Panagopoulos, Eiko Yoneki
 * 44th International Conference on Parallel Processing (ICPP), Beijing 2015
 * 
 * DOI: 10.1109/ICPP.2015.46
 * 
 * https://github.com/giech/pdtl
 */

#include "util.h"
#include "degreehandler.h"
#include "adjacencyhandler.h"

using namespace std;

// Implementation of in-memory triangle listing

// TODO: add inmem.h

class InMemAdjacencyHandler : public AdjacencyHandler {
	public:
		InMemAdjacencyHandler(const string input, 
		                      bool ord = false, 
		                      const char* output = NULL, 
		                      unsigned int bufferSize = DEFAULT_BUF)
			: AdjacencyHandler(input, bufferSize)
		{
			ordered = ord;

			if (output != NULL)
			{
				string outName(output);
				if (outName.size() == 0)
				{
					outName = getOutName(input.c_str());
				}
				b = new FileBuffer(outName, bufferSize);
			}
			else
			{
				b = NULL;
			}
		};

		~InMemAdjacencyHandler()
		{
			if (b != NULL)
			{
				b->close();
				delete b;
			}
		}

		unsigned long long getTriangleCount()
		{
			return triangleCount;
		}

	private:
		FileBuffer* b;
		unsigned long long triangleCount;
		Timer t;
		bool ordered;

		vx** graph;

		virtual void overallSetUp()
		{
			graph = new vx*[graphSize];
			fill(graph, graph + graphSize, (vx*) NULL);
			triangleCount = 0;
		}

		virtual void processPhase()
		{
			vx u;
			for (u = 0; u < graphSize; ++u)
			{
				vx* adj_u = graph[u];
				if (adj_u == NULL)
				{
					continue;
				}

				vx deg_u = adj_u[0];
				++adj_u;

				vx vi;
				for (vi = 0; vi < deg_u; ++vi)
				{
					vx v = adj_u[vi];

					vx* adj_v = graph[v];
					if (adj_v == NULL)
					{
						continue;
					}

					vx deg_v = adj_v[0];
					++adj_v;

					vx* intersection = NULL;

					if (b != NULL)
					{
						intersection = new vx[min(deg_u, deg_v)];
					}

					vx interSize = processIntersection(adj_u, deg_u, adj_v, deg_v, intersection);
					triangleCount += interSize;
					if (b != NULL)
					{
						vx i;
						for (i = 0; i < interSize; ++i)
						{
							b->addToBuffer(u);
							b->addToBuffer(v);
							b->addToBuffer(intersection[i]);
						}

						delete[] intersection;
					}
				}
			}
		}

		virtual bool handleEdge(vx from, vx to, vx deg)
		{
			if (ordered || from < to)
			{
				vx* adj = graph[from];
				if (adj == NULL)
				{
					adj = new vx[deg+1];
					graph[from] = adj;
					adj[0] = 0;
				}
				++adj[0];
				adj[adj[0]] = to;
			}
			return true;
		}

		virtual void phaseSetUp(){}
		virtual void overallTearDown()
		{
			vx i;
			for (i = 0; i < graphSize; ++i)
			{
				vx* adj = graph[i];
				if (adj != NULL)
				{
					delete[] adj;
				}
			}
			delete[] graph;
		}
};

int main(int argc, char* argv[])
{
	Timer t;
	t.start();
	if (argc != 4)
	{
		cerr << "Usage: " << argv[0] << " input output ordered" << endl;
		exit(1);
	}

	string input = string(argv[1]);
	const char* output = atoi(argv[2]) != 0 ? "" : NULL;
	bool ordered = atoi(argv[3]) != 0;


	InMemAdjacencyHandler adj(input, ordered, output);
	adj.processAdjacency(0, MAX_EDGES);
	cout << "Adjacency pass finished in " << t.lap() <<endl;
	cout << "Total number of triangles " << adj.getTriangleCount() << endl;
	return 0;
}


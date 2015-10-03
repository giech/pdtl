# PDTL: Parallel and Distributed Triangle Listing for Massive Graphs

**Ilias Giechaskiel, George Panagopoulos, Eiko Yoneki**, 44th International Conference on Parallel Processing ([ICPP](http://icpp2015.tsinghua.edu.cn/program.html#session-4B)), Beijing, China, Sep. 2015. DOI: [10.1109/ICPP.2015.46](https://dx.doi.org/10.1109/ICPP.2015.46).

This repository contains the following:

## Documents

1. Ilias Giechaskiel's [MPhil Dissertation](documents/giechaskiel-dissertation.pdf).
2. The University of Cambridge Computer Laboratory [Technical Report](documents/pdtl-tr.pdf).
3. The ICPP [Pre-Print](documents/pdtl-icpp-preprint.pdf).
4. The ICPP [Presentation](documents/pdtl-icpp-presentation.pdf).

## Code

All the code is under the [code/src](code/src) directory, with executables under [code/bin](code/bin).

### Source and Compilation

Everything can be compiled by just executing `make` from the [code](code) directory. This will create object files under [code/obj](code/obj), with the executables in [code/bin](code/bin).

If you want to alter the source code, at a high-level, the files are as follows:

* `mgt.[h/cpp]` implements the MGT algorithm with our modifications.
* `localmgt.cpp` contains a main to run MGT locally, while `pdtlclient.cpp` and `pdtlmaster.cpp` implement our distributed PDTL framework.
* `highdegreehandler.[h/cpp]` implements the algorithm for the case when there are high-degree vertices, and `inmem.cpp` implements one of the simple in-memory algorithms.
* `fileparser.[h/cpp]` and `fileconverter.[h/cpp]` implement various parsing and conversion functions, with the main in `parser.cpp`.
* Everything else is used to make the code more modular.

### Binaries and Execution

The [code/bin](code/bin) directory contains all the executable binaries.

With the exception of some of the parser utilities, all binaries require the input in the following format: the graph must be undirected, and is split in 2 files, a degree file (ending in `.deg`), and an adjacency file (ending in `.adj`). The degree file contains the degrees of the vertices as `v, d(v)` (with `v` ranging from `0` to `|V|`), while the adjacency file just stores the neighbors of each vertex in order (and sorted).

This format was chosen for compatibility with the original MGT [implementation](http://www.cse.cuhk.edu.hk/~taoyf/paper/codes/trilist/trilist.zip), which is explained in the [manual](http://www.cse.cuhk.edu.hk/~taoyf/paper/codes/trilist/manual). The format is also explained further in Section V-B of our [paper](documents/pdtl-icpp-preprint.pdf), and a real example is given in [code/graphs](code/graphs).

How to execute the various binaries is discussed below. The inputs and outputs always refer to the base name of the `.deg/.adj` filenames.

#### `inmem.bin`

Use this for an in-memory triangle listing algorithm. Simply execute `inmem.bin input output ordered`, where `input` is the base input name, `output` is a 0 for counting, and non-0 for listing, and `ordered` is non-0 if the adjacency list is already ordered.

#### `highdegreehandler.bin`

Use this to remove high-degree vertices ***before orientation***. Execute `highdegreehandler.bin input output maxdeg report`, where `input` is the (base) input name, `output` is the (base) output name for the low-degree graph, `maxdeg` is the maximum degree to allow in the new graph, and `report` is non-0 for listing (instead of counting).

#### `mgt.bin`

Use this for our version of the MGT algorithm. Execute `mgt.bin filename maxdeg output mem instances`, where `filename` is the (base) input name, `output` is non-0 for listing, `mem` is the maximum memory (in MB) to allocate per thread, and `instances` is the number of threads to use.

`maxdeg` is 0 if orientation has not yet been performed, while it is non-zero when the file is already oriented, and has a maximum out-degree equal to `maxdeg`.

#### `pdtlclient.bin` and `pdtlmaster.bin`

Use these two binaries to execute the distributed version of our algorithms. Run `pdtlclient.bin port delete` on the remote machines, where `port` refers to the port number to listen for incoming connections and `delete` is non-zero to delete the files after the counting/listing for the particular graph has finished. This needs to be manually shutdown, e.g. via Ctrl-C.

`pdtlmaster.bin filename maxdeg memsize instances output ip port mem instances ...` is used to run the master. `filename` is the name of graph, `maxdeg` is as for `mgt.bin` (0 for orientation, non-zero for already oriented), `memsize` and `instances` is the memory (in MB) per thread and number of threads to allocate to the master, and `output` is once again non-0 for listing.

For each client, add the following four arguments: `ip` and `port` for the IPv4 address and port of the client, `instances` for the number of threads, and `mem` the memory (in MB) per thread.

#### `parser.bin`

`parser.bin` contains all the utilities for converting between different file formats. `input` and `output` always refer to the basenames of the input and output graphs.

Run `parser.bin order input output` to order the neighbors of all vertices. This function requires memory proportional to the maximum degree.

Run `parser.bin undirect input output` to convert a directed graph into an undirected graph. This function requires memory proportional to the total number of edges.

Run `parser.bin orient input output [mem] [numthreads]` to orient the given graph. Optionally, add a `mem` parameter to specify the amount of memory to allocate (in MB), per thread (0 for unlimited), and `numthreads` to specify the number of threads.

Run `parser.bin convert input output opt/xstream` to convert the graph from the PDTL format to either `opt` or `xstream` format.

Run `parser.bin parse input output snap/xstream [mem] [vn]` to convert a graph from either the `snap` or the `xstream` format into the format required by PDTL. `mem` optionally specifies the maximum amount of memory to allocate (0 for unlimited), and in the case of `xstream`, `vn` is equal to either 2 or 3, to indicate the type of X-Stream edges used.


### Graphs

The repository also contains an [example](code/graphs) of a real graph in the format required by PDTL.
This is the Berkley-Stanford web graph from the [SNAP Repository](https://snap.stanford.edu/data/web-BerkStan.html).
Note that orientation has not yet been performed on this graph.

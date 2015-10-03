/*
 * PDTL: Parallel and Distributed Triangle Listing for Massive Graphs
 * Ilias Giechaskiel, George Panagopoulos, Eiko Yoneki
 * 44th International Conference on Parallel Processing (ICPP), Beijing 2015
 * 
 * DOI: 10.1109/ICPP.2015.46
 * 
 * https://github.com/giech/pdtl
 */

#pragma once
#include "util.h"

// Utility class for common networking functions

#include <thread>

#include <arpa/inet.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <netdb.h>
#include <unistd.h>

vx readVx(int sock);
void writeVx(int sock, vx v);

unsigned long long readULL(int sock);
void writeULL(int sock, unsigned long long tri);

void readFile(int sock, std::string out);
void writeFile(int sock, std::string in);

void concatenate(std::string baseName, int count);
std::string getName(std::string baseName, int i);

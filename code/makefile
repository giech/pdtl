CXX=g++ 
RM=rm -f
CPPFLAGS=-D_FILE_OFFSET_BITS=64 -Wall -Werror -m64 -O3 -std=c++0x

SRCDIR=src
BINDIR=bin
OBJDIR=obj

SRCS=adjacencyhandler.cpp degreehandler.cpp filebuffer.cpp util.cpp \
fileparser.cpp parserutil.cpp
ALLSRCS = $(SRCS) highdegreehandler.cpp inmem.cpp mgt.cpp networkutil.cpp \
parser.cpp localmgt.cpp pdtlclient.cpp pdtlmaster.cpp loadbalance.cpp threadpool.cpp

OBJS=$(patsubst %.cpp,%.o,$(SRCS))
DEPS=$(patsubst %.o,$(OBJDIR)/%.d,$(SRCS))
ALLOBJS=$(patsubst %.cpp,$(OBJDIR)/%.o,$(ALLSRCS))
ALLDEPS=$(patsubst %.cpp,$(OBJDIR)/%.d,$(ALLSRCS))


all: parser inmem mgt highdegreehandler pdtlmaster pdtlclient

parser: $(OBJS) parser.o fileparser.o fileconverter.o
	$(CXX) -pthread $(patsubst %.o,$(OBJDIR)/%.o,$^) -o $(BINDIR)/$@.bin
	
inmem: $(OBJS) inmem.o
	$(CXX) -pthread $(patsubst %.o,$(OBJDIR)/%.o,$^) -o $(BINDIR)/$@.bin
	
mgt: $(OBJS) mgt.o localmgt.o networkutil.o loadbalance.o threadpool.o
	$(CXX) -pthread $(patsubst %.o,$(OBJDIR)/%.o,$^) -o $(BINDIR)/$@.bin
	
highdegreehandler: $(OBJS) highdegreehandler.o
	$(CXX) -pthread $(patsubst %.o,$(OBJDIR)/%.o,$^) -o $(BINDIR)/$@.bin
	
pdtlmaster: $(OBJS) networkutil.o pdtlmaster.o loadbalance.o mgt.o
	$(CXX) -pthread $(patsubst %.o,$(OBJDIR)/%.o,$^) -o $(BINDIR)/$@.bin
	
pdtlclient: $(OBJS) networkutil.o pdtlclient.o mgt.o
	$(CXX) -pthread $(patsubst %.o,$(OBJDIR)/%.o,$^) -o $(BINDIR)/$@.bin

clean:
	$(RM) $(ALLOBJS) $(ALLDEPS)
		
%.o: $(SRCDIR)/%.cpp
	$(CXX) -c $(CPPFLAGS) -MD -MP -MF $(OBJDIR)/${@:.o=.d}  $<  -o $(OBJDIR)/$@

-include $(DEP)

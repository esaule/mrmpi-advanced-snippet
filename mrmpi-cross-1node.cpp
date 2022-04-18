#include <vector>
#include <iostream>
#include <cmath>
#include <cstdlib>
#include <chrono>
#include <mpi.h>
#include <mapreduce.h>
#include <keyvalue.h>
#include <fstream>

void printLetterKV(char * k, int ks, char *v, int vs, void*) {
  std::cout<<*k<<" "<<*v<<"\n";  
}

void printCrossKV(char * k, int ks, char *v, int vs, void*) {
  std::cout<<*(uint64_t*)k<<" "<<v[8]<<" "<<v[9]<<"\n";  
}

void addChar(int taskid, MAPREDUCE_NS::KeyValue *kv, void *context) {
  char k = taskid+'A';
  char v = taskid+'a';
  kv->add((char*)&k, sizeof(k), (char*)&v, sizeof(v));
}

void addNum(int taskid, MAPREDUCE_NS::KeyValue *kv, void *context) {
  char k = taskid+'1';
  char v = taskid+'!';
  kv->add((char*)&k, sizeof(k), (char*)&v, sizeof(v));
}

void cross0(uint64_t taskid,
	   char * k, int ks,
	   char *v, int vs,
	   MAPREDUCE_NS::KeyValue *kv, void *context) {
  uint64_t copies0 = ((uint64_t*)context)[0];
  uint64_t copies1 = ((uint64_t*)context)[1];

  int bufsize = ks+vs+2*sizeof(int);
  char* buf = new char[bufsize];
  memset (buf, 0, bufsize);
  
  {
    char* base = buf;
    memcpy(base, &ks, sizeof(ks));
    base += sizeof(ks);
    memcpy(base, &vs, sizeof(vs));
    base += sizeof(vs);
    memcpy(base, k, ks);
    base += ks;
    memcpy(base, v, vs);
    base += vs;
  }
  
  for (uint64_t c = 0; c<copies1; ++c) {
    uint64_t id = taskid*copies1+c;

    kv->add((char*)&id, sizeof(id), buf, bufsize);
  }

  delete[] buf;
}

void cross1(uint64_t taskid,
	   char * k, int ks,
	   char *v, int vs,
	   MAPREDUCE_NS::KeyValue *kv, void *context) {
  uint64_t copies0 = ((uint64_t*)context)[0];
  uint64_t copies1 = ((uint64_t*)context)[1];

  int bufsize = ks+vs+2*sizeof(int);
  char* buf = new char[bufsize];
  memset (buf, 0, bufsize);
  
  {
    char* base = buf;
    memcpy(base, &ks, sizeof(ks));
    base += sizeof(ks);
    memcpy(base, &vs, sizeof(vs));
    base += sizeof(vs);
    memcpy(base, k, ks);
    base += ks;
    memcpy(base, v, vs);
    base += vs;
  }
  
  for (uint64_t c = 0; c<copies0; ++c) {
    uint64_t id = taskid+copies1*c;

    kv->add((char*)&id, sizeof(id), buf, bufsize);
  }

  delete[] buf;
}


void printProduct(char * key, int keysize,
		  char * multivalue, int nbvalue, int *valuesize,
		  MAPREDUCE_NS::KeyValue * kv, void * context) {
  char key1 = multivalue[2*sizeof(int)];
  char value1 = multivalue[2*sizeof(int)+1];
  char key2 = multivalue[valuesize[0]+2*sizeof(int)];
  char value2 = multivalue[valuesize[0]+2*sizeof(int)+1];
  std::cout<<*((uint64_t*)key)<<" ("<<key1<<", "<<value1<<" ; "<<key2<<", "<<value2<<")"<<std::endl;
}

int main (int argc, char* argv[]) {

  MPI_Init(&argc, &argv);
  
  MAPREDUCE_NS::MapReduce mr;
  MAPREDUCE_NS::MapReduce mr2;
  MAPREDUCE_NS::MapReduce mrcross;
  
  uint64_t s1 = mr.map(3, addChar, nullptr);
  mr.scan (printLetterKV, nullptr);
  std::cout<<"\n";
  
  uint64_t s2 = mr2.map(5, addNum, nullptr);
  mr2.scan (printLetterKV, nullptr);
  std::cout<<"\n";
  
  uint64_t gs [2];

  gs[0] = s1;
  gs[1] = s2;
 
  mrcross.map(&mr, cross0, (void*)&gs);
  mrcross.map(&mr2, cross1, (void*)&gs, 1);

  mrcross.scan (printCrossKV, nullptr);
  std::cout<<"\n";
  
  mrcross.convert();

  mrcross.reduce(printProduct, nullptr);

  MPI_Finalize();

  return 0;
}


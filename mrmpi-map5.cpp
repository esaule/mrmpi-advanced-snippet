#include <vector>
#include <iostream>
#include <cmath>
#include <cstdlib>
#include <chrono>
#include <mpi.h>
#include <mapreduce.h>
#include <keyvalue.h>
#include <fstream>

void justAdd(int taskid, MAPREDUCE_NS::KeyValue *kv, void *context) {
  std::cout<<"========= "<<taskid<<" ========="<<std::endl;
  
  int k = taskid;
  int v = taskid*10;
  std::cout<<k<<" "<<v<<std::endl;
  kv->add((char*)&k, sizeof(k), (char*)&v, sizeof(v));
  std::cout<<"========= "<<taskid<<" ========="<<std::endl;
}

void justCopy(uint64_t taskid, char * k, int ks, char *v, int vs,  MAPREDUCE_NS::KeyValue *kv, void *context) {
  std::cout<<"========= "<<taskid<<" ========="<<std::endl;
  
  std::cout<<*((int*)k)<<" "<<*((int*)v)<<std::endl;
  kv->add(k, ks, v, vs);
  std::cout<<"========= "<<taskid<<" ========="<<std::endl;
}

int main (int argc, char* argv[]) {

  MPI_Init(&argc, &argv);
  
  MAPREDUCE_NS::MapReduce mr;
  MAPREDUCE_NS::MapReduce mr2;

  
  std::cout<<"map1 (variant 1)"<<std::endl;
  mr.map(5, justAdd, nullptr);
  std::cout<<std::endl<<std::endl<<std::endl;
  
  std::cout<<"map2 (variant 5)"<<std::endl;
  mr2.map(&mr, justCopy, nullptr);
  std::cout<<std::endl<<std::endl<<std::endl;

  
  MPI_Finalize();

  return 0;
}


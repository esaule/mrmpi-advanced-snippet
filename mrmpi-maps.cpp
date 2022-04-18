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


void justPrint(int taskid, char* str, MAPREDUCE_NS::KeyValue* kv, void* ptr) {
  std::cout<<"========= "<<taskid<<" ========="<<std::endl;
  std::cout<<str<<std::endl;  
  std::cout<<"========= "<<taskid<<" ========="<<std::endl;
}

void justPrint2(int taskid, char* str, int sizestr, MAPREDUCE_NS::KeyValue* kv, void* ptr) {
  std::cout<<"========= "<<taskid<<" ========="<<std::endl;
  std::cout<<str<<std::endl;  
  std::cout<<"========= "<<taskid<<" ========="<<std::endl;
}


int main (int argc, char* argv[]) {

  MPI_Init(&argc, &argv);
  
  MAPREDUCE_NS::MapReduce mr;
  MAPREDUCE_NS::MapReduce mr2;

  char* data="data";
  char* a1="data/a1";
  char* a2="data/a2";
  char* b1="data/b/b1";
  char* b2="data/b/b2";

  char* somedata="somedata";

  
  char* list1[2];
  list1[0] = a1;
  list1[1] = a2;

  char* list2[1];
  list2[0] = data;

  char* list3[1];
  list3[0] = somedata;
  
  std::cout<<"map1 (variant 2)"<<std::endl;
  mr.map(2, list1, 0, 0, 0, justPrint, nullptr);
  std::cout<<std::endl<<std::endl<<std::endl;

  std::cout<<"map2 (variant 2) [recurse=0]"<<std::endl;
  mr.map(1, list2, 0, 0, 0, justPrint, nullptr);
  std::cout<<std::endl<<std::endl<<std::endl;

  std::cout<<"map3 (variant 2) [recurse=1]"<<std::endl;
  mr.map(1, list2, 0, 1, 0, justPrint, nullptr);
  std::cout<<std::endl<<std::endl<<std::endl;

  std::cout<<"map3 (variant 2) [readfile=1]"<<std::endl;
  mr.map(1, list3, 0, 0, 1, justPrint, nullptr);
  std::cout<<std::endl<<std::endl<<std::endl;
  
  
  std::cout<<"map4 (variant 3) [delta=30]"<<std::endl;
  mr.map(10, 2, list1, 0, 0, '\n', 30, justPrint2, nullptr);
  std::cout<<std::endl<<std::endl<<std::endl;

  std::cout<<"map5 (variant 3) [delta=10]"<<std::endl;
  mr.map(10, 2, list1, 0, 0, '\n', 10, justPrint2, nullptr);
  std::cout<<std::endl<<std::endl<<std::endl;

  
  std::cout<<"map6 (variant 1)"<<std::endl;
  mr.map(5, justAdd, nullptr);
  std::cout<<std::endl<<std::endl<<std::endl;
  
  std::cout<<"map7 (variant 5)"<<std::endl;
  mr2.map(&mr, justCopy, nullptr);
  std::cout<<std::endl<<std::endl<<std::endl;

  
  MPI_Finalize();

  return 0;
}


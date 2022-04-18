#include <vector>
#include <iostream>
#include <cmath>
#include <cstdlib>
#include <chrono>
#include <mpi.h>
#include <mapreduce.h>
#include <keyvalue.h>
#include <fstream>

void justPrint2(int taskid, char* str, int sizestr, MAPREDUCE_NS::KeyValue* kv, void* ptr) {
  std::cout<<"========= "<<taskid<<": "<<str<<std::endl;  
}

int main (int argc, char* argv[]) {

  MPI_Init(&argc, &argv);
  
  MAPREDUCE_NS::MapReduce mr;

  char* a1="data/a1";
  char* a2="data/a2";

  char* list1[2];
  list1[0] = a1;
  list1[1] = a2;

  
  std::cout<<"map1 (variant 3) [delta=30]"<<std::endl;
  mr.map(10, 2, list1, 0, 0, '\n', 30, justPrint2, nullptr);
  std::cout<<std::endl<<std::endl<<std::endl;

  std::cout<<"map2 (variant 3) [delta=10]"<<std::endl;
  mr.map(10, 2, list1, 0, 0, '\n', 10, justPrint2, nullptr);
  std::cout<<std::endl<<std::endl<<std::endl;

  std::cout<<"map3 (variant 3) [delta=3]"<<std::endl;
  mr.map(10, 2, list1, 0, 0, '\n', 3, justPrint2, nullptr);
  std::cout<<std::endl<<std::endl<<std::endl;

  
  MPI_Finalize();

  return 0;
}


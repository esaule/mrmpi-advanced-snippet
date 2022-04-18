#include <vector>
#include <iostream>
#include <cmath>
#include <cstdlib>
#include <chrono>
#include <mpi.h>
#include <mapreduce.h>
#include <keyvalue.h>
#include <fstream>


void justPrint(int taskid, char* str, MAPREDUCE_NS::KeyValue* kv, void* ptr) {
  std::cout<<"========= "<<taskid<<" "<<str<<std::endl;  
}


int main (int argc, char* argv[]) {

  MPI_Init(&argc, &argv);
  
  MAPREDUCE_NS::MapReduce mr;

  char* a1="data/a1";
  char* a2="data/a2";
  
  char* list1[2];
  list1[0] = a1;
  list1[1] = a2;
  
  std::cout<<"map1 (variant 2)"<<std::endl;
  mr.map(2, list1, 0, 0, 0, justPrint, nullptr);
  std::cout<<std::endl<<std::endl<<std::endl;


  char* data="data";

  char* list2[1];
  list2[0] = data;
  
  std::cout<<"map2 (variant 2) [recurse=0]"<<std::endl;
  mr.map(1, list2, 0, 0, 0, justPrint, nullptr);
  std::cout<<std::endl<<std::endl<<std::endl;

  std::cout<<"map3 (variant 2) [recurse=1]"<<std::endl;
  mr.map(1, list2, 0, 1, 0, justPrint, nullptr);
  std::cout<<std::endl<<std::endl<<std::endl;


  
  char* somedata="somedata";

  char* list3[1];
  list3[0] = somedata;

  std::cout<<"map4 (variant 2) [readfile=1]"<<std::endl;
  mr.map(1, list3, 0, 0, 1, justPrint, nullptr);
  std::cout<<std::endl<<std::endl<<std::endl;
    
  
  MPI_Finalize();

  return 0;
}


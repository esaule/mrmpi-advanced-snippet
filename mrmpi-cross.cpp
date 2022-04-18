#include <vector>
#include <iostream>
#include <cmath>
#include <cstdlib>
#include <chrono>
#include <mpi.h>
#include <mapreduce.h>
#include <keyvalue.h>
#include <fstream>

void addChar(int taskid, MAPREDUCE_NS::KeyValue *kv, void *context) {
  char k = taskid+'A';
  char v = taskid+'a';
  std::cout<<k<<" "<<v<<"\n";
  kv->add((char*)&k, sizeof(k), (char*)&v, sizeof(v));

}

void addNum(int taskid, MAPREDUCE_NS::KeyValue *kv, void *context) {
  char k = taskid+'1';
  char v = taskid+'!';
  std::cout<<k<<" "<<v<<"\n";
  kv->add((char*)&k, sizeof(k), (char*)&v, sizeof(v));
  
}

void cross0(uint64_t taskid,
	   char * k, int ks,
	   char *v, int vs,
	   MAPREDUCE_NS::KeyValue *kv, void *context) {
  uint64_t copies0 = ((uint64_t*)context)[0];
  uint64_t copies1 = ((uint64_t*)context)[1];
  uint64_t offset = ((uint64_t*)context)[2];

  int bufsize = ks+vs+3*sizeof(int);
  char* buf = new char[bufsize];
  memset (buf, 0, bufsize);
  int groupid = 1;
  
  {
    char* base = buf;
    memcpy(base, &groupid, sizeof(groupid));
    base += sizeof(groupid);
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
    uint64_t id = (taskid+offset)*copies1+c;

    kv->add((char*)&id, sizeof(id), buf, bufsize);
    std::cout<<"taskid="<<taskid<<" offset="<<offset<<" copies1="<<copies1<<" c="<<c<<" ==> key="<<id<<"\n";
  }

  delete[] buf;
}

void cross1(uint64_t taskid,
	   char * k, int ks,
	   char *v, int vs,
	   MAPREDUCE_NS::KeyValue *kv, void *context) {
  uint64_t copies0 = ((uint64_t*)context)[0];
  uint64_t copies1 = ((uint64_t*)context)[1];
  uint64_t offset = ((uint64_t*)context)[2];

  int bufsize = ks+vs+3*sizeof(int);
  char* buf = new char[bufsize];
  memset (buf, 0, bufsize);
  int groupid = 2;
  
  {
    char* base = buf;
    memcpy(base, &groupid, sizeof(groupid));
    base += sizeof(groupid);
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
    uint64_t id = taskid+offset+copies1*c;

    kv->add((char*)&id, sizeof(id), buf, bufsize);

    std::cout<<"taskid="<<taskid<<" offset="<<offset<<" copies1="<<copies1<<" c="<<c<<" ==> key="<<id<<"\n";
  }

  delete[] buf;
}


void printProduct(char * key, int keysize,
		  char * multivalue, int nbvalue, int *valuesize,
		  MAPREDUCE_NS::KeyValue * kv, void * context) {
  char* key1;
  int key1size;
  char* value1;
  int value1size;
  
  
  char* key2;
  int key2size;
  char* value2;
  int value2size;

  //it is unclear which of the two entries will come as the first key, so need to disambiguate
  if (((int*)multivalue)[0] == 1) {
    key1 = multivalue+3*sizeof(int);
    key1size=*((int*)(multivalue+sizeof(int)));
    value1 = multivalue+3*sizeof(int)+key1size;
    value1size=*((int*)(multivalue+2*sizeof(int)));

    multivalue+=valuesize[0];

    key2 = multivalue+3*sizeof(int);
    key2size=*((int*)(multivalue+sizeof(int)));
    value2 = multivalue+3*sizeof(int)+key1size;
    value2size=*((int*)(multivalue+2*sizeof(int)));
  }
  else {
    key2 = multivalue+3*sizeof(int);
    key2size=*((int*)(multivalue+sizeof(int)));
    value2 = multivalue+3*sizeof(int)+key1size;
    value2size=*((int*)(multivalue+2*sizeof(int)));

    multivalue+=valuesize[0];

    key1 = multivalue+3*sizeof(int);
    key1size=*((int*)(multivalue+sizeof(int)));
    value1 = multivalue+3*sizeof(int)+key1size;
    value1size=*((int*)(multivalue+2*sizeof(int)));
  }
  
  std::cout<<*((uint64_t*)key)<<" ("<<*key1<<", "<<*value1<<" ; "<<*key2<<", "<<*value2<<")"<<std::endl;
}

void countme(char *, int, char *, int, void *ptr){
  uint64_t* cnt = (uint64_t*)ptr;
  (*cnt)++;
}
						 

int main (int argc, char* argv[]) {

  MPI_Init(&argc, &argv);

  int rank;
  MPI_Comm_rank(MPI_COMM_WORLD, &rank);
  
  MAPREDUCE_NS::MapReduce mr;
  MAPREDUCE_NS::MapReduce mr2;
  MAPREDUCE_NS::MapReduce mrcross;
  
  std::cout<<"===map1===\n"; 
  mr.map(3, addChar, nullptr);

  std::cout<<"===map2===\n";
  mr2.map(5, addNum, nullptr);

  uint64_t s0=0;
  uint64_t s1=0;
  mr.scan(countme, &s0);
  mr2.scan(countme, &s1);
  
  uint64_t gs [3];
  uint64_t pr_gs [3];

  MPI_Allreduce(&s0, &(gs[0]), 1, MPI_LONG, MPI_SUM, MPI_COMM_WORLD);
  MPI_Allreduce(&s1, &(gs[1]), 1, MPI_LONG, MPI_SUM, MPI_COMM_WORLD);

  MPI_Scan(&s0, &(pr_gs[0]), 1, MPI_LONG, MPI_SUM, MPI_COMM_WORLD);
  pr_gs[0] -= s0;

  MPI_Scan(&s1, &(pr_gs[1]), 1, MPI_LONG, MPI_SUM, MPI_COMM_WORLD);
  pr_gs[1] -= s1;
  
  std::cout<<"===map3===\n";
  gs[2] = pr_gs[0];
  mrcross.map(&mr, cross0, (void*)&gs);
  
  std::cout<<"===map4===\n";
  gs[2] = pr_gs[1];
  mrcross.map(&mr2, cross1, (void*)&gs, 1);

  mrcross.collate(nullptr);
  std::cout<<"===reduce===\n";
  
  mrcross.reduce(printProduct, nullptr);

  MPI_Finalize();
  return 0;
}


#include <numa.h>
#include <iostream>
#include <cpuinfo.h>

struct cpuinfo {
  int num_cpus;
  int num_nodes;
  int **node_map;
};

static struct cpuinfo cpu_info;

void*
alloc_huge(size_t size) {
  numa_set_strict(1);
  void *ret = mmap(MMAP_ADDR, size, MMAP_PROT, MMAP_FLAGS, 0, 0);
  if (ret == MAP_FAILED){
    std::cout << "HUGE PAGE ALLOCATION FAILED!\n";
    exit(-1);
  }    
  if (mlock(ret, size) < 0) {
    //    numa_free(buf, size);
    std::cout << "mlock couldn't pin memory to RAM!\n";
    exit(-1);
  }
  else {
    return ret;
  }
}

void
init_cpuinfo() {
  int i;
  int num_cpus = numa_num_thread_cpus();
  int num_numa_nodes = numa_num_thread_nodes();
  cpu_info.num_nodes = num_numa_nodes;    
  cpu_info.num_cpus = num_cpus;

  // Keep a count of how many cpus exist per node. 
  int cpu_count[num_numa_nodes];
  for (i = 0; i < num_numa_nodes; ++i) 
    cpu_count[i] = 0;
    
    
  // Allocate an array of arrays to store information for each numa
  // node in the system. 
  cpu_info.node_map = (int **)malloc(sizeof(int*) * num_numa_nodes);    

    
  // First pass, get the number of cpus per node. 
  for (i = 0; i < num_cpus; ++i) 
    cpu_count[numa_node_of_cpu(i)] += 1;

  // Initialize an array corresponding to each numa node to store
  // cpu information. 
  for (i = 0; i < num_numa_nodes; ++i) {
    cpu_info.node_map[i] = (int *)malloc(sizeof(int) * (1+cpu_count[i]));
    cpu_info.node_map[i][0] = cpu_count[i];
    cpu_count[i] = 1;
  }
    
  // Second pass, map each cpu to its corresponding numa node. 
  for (i = 0; i < num_cpus; ++i) {
    int numa_node = numa_node_of_cpu(i);
    cpu_info.node_map[numa_node][cpu_count[numa_node]] = i;
    cpu_count[numa_node] += 1;
  }
}

int
get_num_cpus() {
  return cpu_info.num_cpus;
}

// Assumes that it's never called on an index greater than a valid
// cpu number. 
int
get_cpu(int index, int striped) {
  int node, cpu_index;
  if (striped) {
    node = index % cpu_info.num_nodes;
    cpu_index = index / cpu_info.node_map[node][0];
  }
  else {
    node = index / cpu_info.num_nodes;
    cpu_index = index % cpu_info.num_nodes;
  }
  return cpu_info.node_map[node][1+cpu_index];
}

int
pin_thread(int cpu) {
  numa_set_strict(1);
  cpu_set_t binding;
  CPU_ZERO(&binding);
  CPU_SET(cpu, &binding);

  // Kill the program if we can't bind. 
  pthread_t self = pthread_self();
  if (pthread_setaffinity_np(self, sizeof(cpu_set_t), &binding) < 0) {
    std::cout << "Couldn't bind to my cpu!\n";
    exit(-1);
  }
  return 0;
}

void*
alloc_mem(size_t size, int cpu) {
  if (TESTING) {
    return malloc(size);
  }
  else {
    int numa_node = numa_node_of_cpu(cpu);
    //  std::cout << "memory allocated on node: " << numa_node << "\n";
    numa_set_strict(1);
    void *buf = numa_alloc_onnode(size, numa_node);
    return buf;
    if (mlock(buf, size) < 0) {
      numa_free(buf, size);
      std::cout << "mlock couldn't pin memory to RAM!\n";
      return NULL;
    } 
    else {
      return buf;
    }
  }
}

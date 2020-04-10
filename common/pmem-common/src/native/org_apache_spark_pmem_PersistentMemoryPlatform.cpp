#include <memkind.h>
#include <stdexcept>
#include <cstring>
#include <cstdio>
#include <cstdlib>
#include <cassert>
#include <iostream>
#include <exception>
#include <unordered_map>
#include "org_apache_spark_pmem_PersistentMemoryPlatform.h"

memkind *pmemkind=NULL;

// copied form openjdk: http://hg.openjdk.java.net/jdk8/jdk8/hotspot/file/87ee5ee27509/src/share/vm/prims/unsafe.cpp
inline void* addr_from_java(jlong addr) {
  // This assert fails in a variety of ways on 32-bit systems.
  // It is impossible to predict whether native code that converts
  // pointers to longs will sign-extend or zero-extend the addresses.
  //assert(addr == (uintptr_t)addr, "must not be odd high bits");
  return (void*)(uintptr_t)addr;
}

inline jlong addr_to_java(void* p) {
  //assert(p == (void*)(uintptr_t)p, "must not be odd high bits");
  assert(p == (void*)(uintptr_t)p);
  return (uintptr_t)p;
}

JNIEXPORT void JNICALL Java_org_apache_spark_pmem_PersistentMemoryPlatform_initializeNative
  (JNIEnv *env, jclass clazz, jstring path, jlong size) {
  const char* str = env->GetStringUTFChars(path, NULL);
  if (NULL == str) {
    throw std::invalid_argument("vm path can't be NULL.\n");
  }

  size_t sz = (size_t)size;
  int error = memkind_create_pmem(str, sz, &pmemkind);
  if (error) {
    throw std::runtime_error("memkind_create failed!\n");
  } else {
    std::cout << "memkind created!" << std::endl;
  }

  env->ReleaseStringUTFChars(path, str);
}

JNIEXPORT jlong JNICALL Java_org_apache_spark_pmem_PersistentMemoryPlatform_allocateMemory
  (JNIEnv *env, jclass clazz, jlong size) {
  if (NULL == pmemkind) {
    throw std::invalid_argument("VMEM is NULL--allocateMemory.\n");
  }

  size_t sz = (size_t)size;
  void *p = memkind_malloc(pmemkind, sz);
  if (p == NULL) {
    throw std::runtime_error("Out of memory!\n");
  }

  return addr_to_java(p);
}

JNIEXPORT void JNICALL Java_org_apache_spark_pmem_PersistentMemoryPlatform_freeMemory
  (JNIEnv *env, jclass clazz, jlong address) {
  if (NULL == pmemkind) {
    throw std::invalid_argument("VMEM is NULL--freeMemory.\n");
  }

  memkind_free(pmemkind, addr_from_java(address));
}

JNIEXPORT void JNICALL Java_org_apache_spark_pmem_PersistentMemoryPlatform_copyMemory
  (JNIEnv *env, jclass clazz, jlong destination, jlong source, jlong size) {
  size_t sz = (size_t)size;
  void *dest = addr_from_java(destination);
  void *src = addr_from_java(source);
  std::memcpy(dest, src, sz);
}
// the lock server implementation

#include "lock_server.h"
#include <sstream>
#include <stdio.h>
#include <unistd.h>
#include <arpa/inet.h>

#define PRINT_DEBUG false

lock_server::lock_server():
  nacquire (0), locks_held(), mutex(PTHREAD_MUTEX_INITIALIZER), cond(PTHREAD_COND_INITIALIZER)
{
  pthread_mutex_init(&mutex, NULL);
  pthread_cond_init(&cond, NULL);
}

lock_protocol::status
lock_server::stat(int clt, lock_protocol::lockid_t lid, int &r)
{
  lock_protocol::status ret = lock_protocol::OK;
  if(PRINT_DEBUG) printf("stat request from clt %d\n", clt);
  r = nacquire;
  return ret;
}

lock_protocol::status
lock_server::acquire(int clt, lock_protocol::lockid_t lid, int &r)
{
  lock_protocol::status ret = lock_protocol::OK;
  if(PRINT_DEBUG) printf("acquire request of lock %d from clt %d\n", (int)lid, clt);
  
  pthread_mutex_lock(&mutex);
  
  while (1) {

    // no other client holds lock =lid=
    std::map<lock_protocol::lockid_t, int>::iterator result = locks_held.find(lid);
    if( result == locks_held.end() ) {
      
      // then get it!
      locks_held[lid] = clt;
      r = nacquire;
      break;
    }

    // otherwise, wait for something to be released
    pthread_cond_wait(&cond, &mutex);

  }

  pthread_mutex_unlock(&mutex);
  return ret;
}

lock_protocol::status
lock_server::release(int clt, lock_protocol::lockid_t lid, int &r)
{
  lock_protocol::status ret = lock_protocol::OK;
  if(PRINT_DEBUG) printf("release request of lock %d from clt %d\n", (int)lid, clt);

  pthread_mutex_lock(&mutex);

  std::map<lock_protocol::lockid_t, int>::iterator result = locks_held.find(lid);
  if( result != locks_held.end() || (*result).second == clt ) {
    locks_held.erase(lid);
  }

  pthread_cond_broadcast(&cond);
  pthread_mutex_unlock(&mutex);
  
  r = nacquire;
  return ret;
}



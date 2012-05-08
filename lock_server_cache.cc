// the caching lock server implementation

#include "lock_server_cache.h"
#include <sstream>
#include <stdio.h>
#include <unistd.h>
#include <arpa/inet.h>
#include "lang/verify.h"
#include "handle.h"
#include "tprintf.h"

#define SERVER_PRINT_DEBUG false

lock_server_cache::lock_server_cache()
{
  pthread_mutex_init(&locks_mutex, NULL);
}


int lock_server_cache::acquire(lock_protocol::lockid_t lid, std::string id, 
                               int &)
{
  lock_protocol::status ret = lock_protocol::OK;
  if(SERVER_PRINT_DEBUG) printf("server: acq lock %d from client %s\n", lid, id.c_str());
  pthread_mutex_lock(&locks_mutex);
  server_lock_t &lock = locks[lid];
  if(SERVER_PRINT_DEBUG) printf("server: lock %d is currently %d-locked by %s\n", lid, lock.is_locked, lock.holder.c_str());
  
  if( ! clients.count(id) ) {
    sockaddr_in dstsock;
    make_sockaddr(id.c_str(), &dstsock);
    rpcc *cl = new rpcc(dstsock);
    if( cl->bind() < 0 ) {
      if(SERVER_PRINT_DEBUG) printf("Problem binding to client %s\n", id.c_str());
    }
    clients[id] = cl;
  }

  bool revoke = false;
  if( ! lock.is_locked ) {
    lock.is_locked = true;
    lock.holder = id;
    lock.waiting_set.erase(id);
    if(SERVER_PRINT_DEBUG) printf("server: sending lock %d to client %s\n", lid, id.c_str());
    if(lock.waiting_set.size()) revoke = true;
  }
  else {
    lock.waiting_set.insert(id);
    revoke = true;
    ret = lock_protocol::RETRY;
  }
  if(revoke) {
    pthread_mutex_unlock(&locks_mutex);
    if(SERVER_PRINT_DEBUG) printf("server: revoking lock %d from client %s (%s is waiting)\n", lid, lock.holder.c_str(), lock.waiting_set.begin()->c_str());
    int r;
    rlock_protocol::status rval = clients[lock.holder]->call(rlock_protocol::revoke, lid, r);
    VERIFY( rval == rlock_protocol::OK );
  }
  else {
    pthread_mutex_unlock(&locks_mutex);
  }
  return ret;
}

int 
lock_server_cache::release(lock_protocol::lockid_t lid, std::string id, 
         int &)
{
  lock_protocol::status ret = lock_protocol::OK;
  pthread_mutex_lock(&locks_mutex);
  server_lock_t &lock = locks[lid];
  if( lock.is_locked && lock.holder.compare(id) == 0 ) {
    if(SERVER_PRINT_DEBUG) printf("server: releasing lock %d, previously held by client %s\n", lid, id.c_str());
    lock.is_locked = false;
    if( ! lock.waiting_set.empty() ) {
      std::string next_client = *(lock.waiting_set.begin());
      if(SERVER_PRINT_DEBUG) printf("server: sending retry on lock %d to client %s\n", lid, next_client.c_str());
      pthread_mutex_unlock(&locks_mutex);
      int r;
      rlock_protocol::status rval = clients[next_client]->call(rlock_protocol::retry, lid, r);
      VERIFY (rval == rlock_protocol::OK);
      return ret;
    }
  }
  pthread_mutex_unlock(&locks_mutex);
  return ret;
}

lock_protocol::status
lock_server_cache::stat(lock_protocol::lockid_t lid, int &r)
{
  tprintf("stat request\n");
  r = nacquire;
  pthread_mutex_lock(&locks_mutex);
  pthread_mutex_unlock(&locks_mutex);
  return lock_protocol::OK;
}

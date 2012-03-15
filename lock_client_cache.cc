// RPC stubs for clients to talk to lock_server, and cache the locks
// see lock_client.cache.h for protocol details.

#include "lock_client_cache.h"
#include "rpc.h"
#include <sstream>
#include <iostream>
#include <stdio.h>
#include "tprintf.h"


lock_client_cache::lock_client_cache(std::string xdst, 
				     class lock_release_user *_lu)
  : lock_client(xdst), lu(_lu)
{
  rpcs *rlsrpc = new rpcs(0);
  rlsrpc->reg(rlock_protocol::revoke, this, &lock_client_cache::revoke_handler);
  rlsrpc->reg(rlock_protocol::retry, this, &lock_client_cache::retry_handler);

  const char *hname;
  hname = "127.0.0.1";
  std::ostringstream host;
  host << hname << ":" << rlsrpc->port();
  id = host.str();

  pthread_mutex_init(&cache_mutex, NULL);
}

lock_protocol::status
lock_client_cache::acquire(lock_protocol::lockid_t lid)
{
  int ret = lock_protocol::OK;
  pthread_mutex_lock(&cache_mutex);
  printf("client %s: acq lock %d\n", id.c_str(), lid);
  

  if( ! cached_locks.count(lid) ) {
    // initialize lock in table.
    printf("client %s: intializing lock %d\n", id.c_str(), lid);
    cached_locks[lid].status = NONE;
    cached_locks[lid].revoked = false;
    //    pthread_mutex_init(&cache.lock_mutex, NULL);
    pthread_cond_init(&cached_locks[lid].lock_cond, NULL);
  }

  cache_lockinfo_t &cache = cached_locks[lid];
  // pthread_mutex_lock(&cache.lock_mutex);

  printf("client %s: lock %d cache status %d; [NONE(%d), FREE(%d), LOCKED(%d), ACQUIRING(%d), RELEASING(%d)]\n",
  	 id.c_str(), lid, cache.status, NONE, FREE, LOCKED, ACQUIRING, RELEASING);
  while( true ) {
    if( cache.status == NONE ) {
      cache.status = ACQUIRING;
      // printf("set ACQUIRING\n");
      //    pthread_mutex_unlock(&cache.lock_mutex);
      pthread_mutex_unlock(&cache_mutex);
      printf("client %s: sending acq rpc for lock %d [%d]\n", id.c_str(), lid, cache.status);
      int r;
      ret = cl->call(lock_protocol::acquire, lid, id, r);
    
      pthread_mutex_lock(&cache_mutex);
      //    pthread_mutex_lock(&cache.lock_mutex);
      if( ret == lock_protocol::OK ) {
	cache.status = LOCKED;
	cache.holder = pthread_self();
	printf("client %s: gave out lock %d\n", id.c_str(), lid);
	break;
      }
      else if( ret == lock_protocol::RETRY ){
	if( cache.status == ACQUIRING ) {
	  pthread_cond_wait(&cache.lock_cond, &cache_mutex);
	  if( cache.status == ACQUIRING ) {
	    cache.status = NONE;
	  //printf("NONE\n");
	  }
	}	  
      }
      else {
	printf("RPC ERROR?\n");
	//break;
      }
    }
    else if( cache.status == FREE ) {
      cache.status = LOCKED;
      //printf("LOCKED!\n");
      cache.holder = pthread_self();
      printf("client %s: gave out cached lock %d\n", id.c_str(), lid);
      break;
    }
    else {
      pthread_cond_wait(&cache.lock_cond, &cache_mutex);
    }
  }

  // printf("end of acquire: cache status %d\n", cache.status);

  //  pthread_mutex_unlock(&cache.lock_mutex);
  pthread_mutex_unlock(&cache_mutex);
  return ret;
}

lock_protocol::status
lock_client_cache::release(lock_protocol::lockid_t lid)
{
  int ret = lock_protocol::OK;
  pthread_mutex_lock(&cache_mutex);
  if( cached_locks.count(lid) ) {
    cache_lockinfo_t &cache = cached_locks[lid];
    printf("client %s: release lock %d [%d]\n", id.c_str(), lid, cache.status);
    if( cache.status == LOCKED && cache.holder == pthread_self() ) {
      if( cache.revoked ) {
	cache.status = RELEASING;
	pthread_mutex_unlock(&cache_mutex);
	printf("client %s: rpc release lock %d\n", id.c_str(), lid);
	int r;
	ret = cl->call(lock_protocol::release, lid, id, r);
	if( ret != lock_protocol::OK )
	  return ret;
	pthread_mutex_lock(&cache_mutex);
	cache.status = NONE;
	cache.revoked = false;
      }
      else {
	printf("client %s: freeing local lock %d\n", id.c_str(), lid);
	cache.status = FREE;
      }
      pthread_cond_broadcast(&cache.lock_cond);
    }
  }
  pthread_mutex_unlock(&cache_mutex);
  return ret;
}

rlock_protocol::status
lock_client_cache::revoke_handler(lock_protocol::lockid_t lid, 
                                  int &)
{
  int ret = rlock_protocol::OK;
  pthread_mutex_lock(&cache_mutex);
  if( cached_locks.count(lid) ) {
    cache_lockinfo_t &cache = cached_locks[lid];
    printf("client %s: revoke lock %d [%d]\n", id.c_str(), lid, cache.status);
    // printf("REVOKING at client; STATUS = %d\n", cache.status);
    if( cache.status == FREE ) {
      cache.status = NONE;
      pthread_mutex_unlock(&cache_mutex);
      printf("client %s: rpc release lock %d\n", id.c_str(), lid);
      int r;
      ret = cl->call(lock_protocol::release, lid, id, r);
      if( ret != lock_protocol::OK )
	return ret;
      pthread_mutex_lock(&cache_mutex);
      pthread_cond_broadcast(&cache.lock_cond);     
    }
    else if( cache.status != RELEASING ) {
      // printf("set to RELEASING\n");
      cache.revoked = true;
    }
  }
  pthread_mutex_unlock(&cache_mutex);
  return ret;
}

rlock_protocol::status
lock_client_cache::retry_handler(lock_protocol::lockid_t lid, 
                                 int &)
{
  int ret = rlock_protocol::OK;
  pthread_mutex_lock(&cache_mutex);
  printf("client %s: retry lock %d\n", id.c_str(), lid);
  if( cached_locks.count(lid) ) {
    cache_lockinfo_t &cache = cached_locks[lid];
    if( cache.status == ACQUIRING ) {
      cache.status = NONE;
      pthread_cond_broadcast(&cache.lock_cond);
    }
  }
  pthread_mutex_unlock(&cache_mutex);
  return ret;
}




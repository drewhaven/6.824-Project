// RPC stubs for clients to talk to extent_server

#include "extent_client.h"
#include "lock_protocol.h"
#include "handle.h"
#include <sstream>
#include <iostream>
#include <stdio.h>
#include <unistd.h>
#include <time.h>

#define EXT_CL_PRINT_DEBUG false

/* P2P: Changed - never have to flush to server anymore, but now calls push */
void
extent_lock_release_user::dorelease(lock_protocol::lockid_t lid, std::string client_id)
{
  //ec->flush(lid);
  ec->push(lid, client_id);
}

void
extent_lock_release_user::push_extent(extent_protocol::extentid_t eid, std::string extent, extent_protocol::attr attr)
{
  ec->received_extent(eid, extent, attr);
}

// The calls assume that the caller holds a lock on the extent
void
extent_client::received_extent(extent_protocol::extentid_t eid, std::string extent, extent_protocol::attr attr)
{
  extent_cache[eid].buf = extent;
  extent_cache[eid].attr = attr;
  extent_cache[eid].attr.atime = time(NULL);
}

extent_client::extent_client(std::string dst)
{
  //sockaddr_in dstsock;
  //make_sockaddr(dst.c_str(), &dstsock);
  //cl = new rpcc(dstsock);
  //if (cl->bind() != 0) {
  //  printf("extent_client: bind failed\n");
  //}

  pthread_mutex_init(&m, NULL);
}

/* P2P This will only be called if we already have the extent so no more RPC call */
extent_protocol::status
extent_client::get(extent_protocol::extentid_t eid, std::string &buf)
{
  extent_protocol::status ret = extent_protocol::OK;
  pthread_mutex_lock(&m);
  buf = extent_cache[eid].buf;
/*  If(EXT_CL_PRINT_DEBUG) printf("get %d [%d]\n", eid, extent_cache.count(eid));
  if( extent_cache.count(eid) ) {
    // return cached copy
    if(EXT_CL_PRINT_DEBUG) printf("... from cache\n");
    buf = extent_cache[eid].buf;
    extent_cache[eid].attr.atime = time(NULL);
  }
  else {
    if(EXT_CL_PRINT_DEBUG) printf("... from server\n");
    ret = cl->call(extent_protocol::get, eid, buf);
    if( ret == extent_protocol::OK ) { // cache it
      extent_cache[eid].buf = buf;
      extent_cache[eid].dirty = false;
      cl->call(extent_protocol::getattr, eid, extent_cache[eid].attr);
    }
  }
  if(EXT_CL_PRINT_DEBUG) printf("... returning %d\n", ret);
  */
  pthread_mutex_unlock(&m);
  return ret;
}

extent_protocol::status
extent_client::getattr(extent_protocol::extentid_t eid, 
		       extent_protocol::attr &attr)
{
  extent_protocol::status ret = extent_protocol::OK;
  pthread_mutex_lock(&m);
  attr = extent_cache[eid].attr;
  // probably should update atime too

  /*
  if(EXT_CL_PRINT_DEBUG) printf("getattr %d [%d]\n", eid, extent_cache.count(eid));
  if( extent_cache.count(eid) ) {
    if(EXT_CL_PRINT_DEBUG) printf("... from cache\n");
    attr = extent_cache[eid].attr;
  }
  else {
    if(EXT_CL_PRINT_DEBUG) printf("... from server\n");
    ret = cl->call(extent_protocol::getattr, eid, attr);
    if( ret == extent_protocol::OK ) { // cache it
      extent_cache[eid].attr = attr;
      extent_cache[eid].dirty = false;
      cl->call(extent_protocol::get, eid, extent_cache[eid].buf);
    }
  }
  */
  pthread_mutex_unlock(&m);
  return ret;
}

extent_protocol::status
extent_client::put(extent_protocol::extentid_t eid, std::string buf)
{
  extent_protocol::status ret = extent_protocol::OK;
  pthread_mutex_lock(&m);
  /*
  if(EXT_CL_PRINT_DEBUG) printf("put %d\n", eid);
  extent &ex = extent_cache[eid];
  ex.buf = buf;
  ex.dirty = true;

  unsigned int t = time(NULL);
  ex.attr.ctime = t;
  ex.attr.mtime = t;
  ex.attr.size = buf.size();
  */

  extent &ex = extent_cache[eid];
  ex.buf = buf;

  unsigned int t = time(NULL);
  ex.attr.ctime = t;
  ex.attr.mtime = t;
  ex.attr.size = buf.size();

  pthread_mutex_unlock(&m);
  return ret;
}

extent_protocol::status
extent_client::remove(extent_protocol::extentid_t eid)
{
  extent_protocol::status ret = extent_protocol::OK;
  pthread_mutex_lock(&m);
  /* P2P
  if(EXT_CL_PRINT_DEBUG) printf("remove %d\n", eid);
  int r;
  ret = cl->call(extent_protocol::remove, eid, r);
  */
  extent_cache.erase(eid);
  pthread_mutex_unlock(&m);
  return ret;
}

/* P2P Deprecated */
extent_protocol::status
extent_client::flush(extent_protocol::extentid_t eid)
{
  extent_protocol::status ret = extent_protocol::OK;
  pthread_mutex_lock(&m);
  if(EXT_CL_PRINT_DEBUG) printf("flush %d [%d]\n", eid, extent_cache.count(eid));
  if( extent_cache.count(eid) ){//&& extent_cache[eid].dirty ) {
    if(EXT_CL_PRINT_DEBUG) printf("... sending put to server\n");
    int r;
    ret = cl->call(extent_protocol::put, eid, extent_cache[eid].buf, r);
  }
  extent_cache.erase(eid);
  pthread_mutex_unlock(&m);
  return ret;
}

void
extent_client::push(lock_protocol::lockid_t lid, std::string client_id)
{
  handle h(client_id);
  rpcc *client = h.safebind();
  if( client ) {
    pthread_mutex_lock(&m);
    extent_protocol::extentid_t eid = lid; // convert from lid to eid
    extent &ex = extent_cache[eid];
    int r;
    client->call(rlock_protocol::push, lid, eid, ex.buf, ex.attr, r);
    pthread_mutex_unlock(&m);
  }
}

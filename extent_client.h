// extent client interface.

#ifndef extent_client_h
#define extent_client_h

#include <map>
#include <string>
#include <pthread.h>
#include "extent_protocol.h"
#include "lock_client_cache.h"
#include "rpc.h"
#include <stdio.h>


class extent_client {
 private:
  rpcc *cl;

  struct extent {
    std::string buf;
    extent_protocol::attr attr;
    // P2P bool dirty;
  };

  pthread_mutex_t m;
  std::map<extent_protocol::extentid_t, extent> extent_cache;

 public:
  extent_client(std::string dst);

  extent_protocol::received_extent(extent_protocol::extentid_t, std::string, extent_protocol::);
  extent_protocol::status get(extent_protocol::extentid_t eid,
			      std::string &buf);
  extent_protocol::status getattr(extent_protocol::extentid_t eid,
				  extent_protocol::attr &a);
  extent_protocol::status put(extent_protocol::extentid_t eid, std::string buf);
  extent_protocol::status remove(extent_protocol::extentid_t eid);
  extent_protocol::status flush(extent_protocol::extentid_t eid);
};

class extent_lock_release_user : public lock_release_user {
private:
  extent_client* ec;
public:
  extent_lock_release_user(extent_client* ec) : ec(ec) { }
  void dorelease(lock_protocol::lockid_t, std:string);
  void push_extent(extent_protocol::extentid_t, std::string);
};

#endif 


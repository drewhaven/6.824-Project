#ifndef lock_server_cache_h
#define lock_server_cache_h

#include <pthread.h>
#include <string>
#include <set>
#include <map>
#include "lock_protocol.h"
#include "rpc.h"
#include "lock_server.h"


class lock_server_cache {
 private:
  int nacquire;
  struct server_lock_t {
    bool is_locked;
    std::string holder;
    std::set<std::string> waiting_set;
  };
  std::map<lock_protocol::lockid_t, server_lock_t> locks;
  std::map<std::string, rpcc*> clients;
  pthread_mutex_t locks_mutex;
 public:
  lock_server_cache();
  lock_protocol::status stat(lock_protocol::lockid_t, int &);
  int acquire(lock_protocol::lockid_t, std::string id, int &);
  int release(lock_protocol::lockid_t, std::string id, int &);
};

#endif

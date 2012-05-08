// lock client interface.

#ifndef lock_client_cache_h

#define lock_client_cache_h

#include <map>
#include <pthread.h>
#include <string>
#include "extent_protocol.h"
#include "lock_protocol.h"
#include "rpc.h"
#include "lock_client.h"
#include "lang/verify.h"

// Classes that inherit lock_release_user can override dorelease so that 
// that they will be called when lock_client releases a lock.
// You will not need to do anything with this class until Lab 5.
class lock_release_user {
 public:
  virtual void dorelease(lock_protocol::lockid_t, std::string) = 0;
  virtual void push_extent(extent_protocol::extentid_t, std::string) = 0;
  virtual ~lock_release_user() {};
};

class lock_client_cache : public lock_client {
 private:
  class lock_release_user *lu;
  int rlock_port;
  std::string hostname;
  std::string id;
 public:
  lock_client_cache(std::string xdst, class lock_release_user *l = 0);
  virtual ~lock_client_cache() {};
  lock_protocol::status acquire(lock_protocol::lockid_t);
  lock_protocol::status release(lock_protocol::lockid_t);
  rlock_protocol::status revoke_handler(lock_protocol::lockid_t, 
                                        std::string, int &);
  rlock_protocol::status retry_handler(lock_protocol::lockid_t, 
                                       int &);
  rlock_protocol::status push_handler(extent_protocol::extentid_t, std::string, int &);

 private:
  enum cachestatus { NONE, FREE, LOCKED, ACQUIRING, RELEASING };
  struct cache_lockinfo_t {
    int status;
    bool revoked;
    pthread_t holder;
    pthread_cond_t lock_cond;
  };
  
  pthread_mutex_t cache_mutex;
  std::map<lock_protocol::lockid_t, cache_lockinfo_t> cached_locks;

};


#endif

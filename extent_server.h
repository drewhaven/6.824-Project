// this is the extent server

#ifndef extent_server_h
#define extent_server_h

#include <string>
#include <map>
#include <pthread.h>
#include "extent_protocol.h"

class extent_server {

 protected:
  pthread_mutex_t m_;

  struct extent {
    std::string str;
    extent_protocol::attr attr;
  };

  std::map<extent_protocol::extentid_t, extent> extents_map_;

 public:
  extent_server();

  int put(extent_protocol::extentid_t id, std::string, int &);
  int get(extent_protocol::extentid_t id, std::string &);
  int getattr(extent_protocol::extentid_t id, extent_protocol::attr &);
  int remove(extent_protocol::extentid_t id, int &);
};

#endif 








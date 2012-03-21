// the extent server implementation

#include "extent_server.h"
#include <ctime>
#include <sstream>
#include <stdio.h>
#include <unistd.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>

extent_server::extent_server()
{
  pthread_mutex_init(&m_, NULL);
  int i; put(1, "", i);
}


int extent_server::put(extent_protocol::extentid_t id, std::string buf, int &)
{
  pthread_mutex_lock(&m_);
  
  extent &ex = extents_map_[id];
  ex.str = buf;
  
  unsigned int t = time(NULL);
  ex.attr.ctime = t;
  ex.attr.mtime = t;
  ex.attr.size = buf.size();

  pthread_mutex_unlock(&m_);
  
  return extent_protocol::OK;
}

int extent_server::get(extent_protocol::extentid_t id, std::string &buf)
{
  pthread_mutex_lock(&m_);

  int ret = extent_protocol::IOERR;

  if(extents_map_.count(id)) {
    extent &ex = extents_map_[id];
    buf = ex.str;
    unsigned int t = time(NULL);
    ex.attr.atime = t;
    ret = extent_protocol::OK; 
  }
  else {
    ret = extent_protocol::NOENT;
  }

  pthread_mutex_unlock(&m_);
  
  return ret;
}

int extent_server::getattr(extent_protocol::extentid_t id, extent_protocol::attr &a)
{
  pthread_mutex_lock(&m_);

  int ret = extent_protocol::IOERR;

  if(extents_map_.count(id)) {
    extent &ex = extents_map_[id];
    a = ex.attr;
    ret = extent_protocol::OK;
  }
  else {
    ret = extent_protocol::NOENT;
  }

  pthread_mutex_unlock(&m_);
  
  return ret;
}

int extent_server::remove(extent_protocol::extentid_t id, int &)
{
  pthread_mutex_lock(&m_);

  extents_map_.erase(id);

  pthread_mutex_unlock(&m_);
  
  return extent_protocol::OK;
}


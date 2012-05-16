// yfs client.  implements FS operations using extent and lock server
#include "yfs_client.h"
#include "extent_client.h"
#include "lock_client_cache.h"
#include <algorithm>
#include <sstream>
#include <iostream>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>


yfs_client::yfs_client(std::string extent_dst, std::string lock_dst)
{
  ec = new extent_client(extent_dst);
  lc = new lock_client_cache(lock_dst, new extent_lock_release_user(ec));
  //srand(time(NULL));  
  std::cerr << "Starting a yfs_client against " << lock_dst << std::endl;
}

yfs_client::inum
yfs_client::n2i(std::string n)
{
  std::istringstream ist(n);
  unsigned long long finum;
  ist >> finum;
  return finum;
}

std::string
yfs_client::filename(inum inum)
{
  std::ostringstream ost;
  ost << inum;
  return ost.str();
}

// @dir is a string of the form
// filename1 NULL inum1 NULL
// filename2 NULL inum2 NULL
// ...
// filenameN NULL inumN NULL NULL
yfs_client::inum
yfs_client::find_in_dir(std::string file, std::string dir)
{
  const char *f = file.c_str(), *d = dir.c_str();
  while( *d != NULL ) {
    if( strcmp(f, d) == 0 ) { // check if filename matches
      d += strlen(d) + 1;
      return n2i(std::string(d)); // return matching inum
    }
    d += strlen(d) + 1; // skip filename
    d += strlen(d) + 1; // skip inum
  }
  return false;
}

std::string
yfs_client::add_to_dir(std::string file, inum ino, std::string dir)
{
  std::ostringstream ost;
  ost << file << std::string(1, NULL) << ino << std::string(1, NULL) << dir;
  return ost.str();
}

std::string
yfs_client::remove_from_dir(std::string file, std::string dir)
{
  const char *f = file.c_str(), *d = dir.c_str();
  int n, pos = 0;
  while( *d != NULL ) {
    if( strcmp(f, d) == 0 ) { // check if filename matches
      n = strlen(d) + 1;
      d += n;
      n += strlen(d) + 1;
      dir.erase(pos, n);
      return dir;
    }
    n = strlen(d) + 1; d += n; pos += n; // skip filename
    n = strlen(d) + 1; d += n; pos += n;// skip inum
  }
  return dir;
}

bool
yfs_client::isfile(inum inum)
{
  if(inum & 0x80000000)
    return true;
  return false;
}

bool
yfs_client::isdir(inum inum)
{
  return ! isfile(inum);
}

int
yfs_client::getfile(inum inum, fileinfo &fin)
{
  int r = OK;
  lc->acquire(inum);

  printf("getfile %016llx\n", inum);
  extent_protocol::attr a;
  if (ec->getattr(inum, a) != extent_protocol::OK) {
    r = IOERR;
    goto release;
  }

  fin.atime = a.atime;
  fin.mtime = a.mtime;
  fin.ctime = a.ctime;
  fin.size = a.size;
  printf("getfile %016llx -> sz %llu\n", inum, fin.size);

 release:
  lc->release(inum);
  return r;
}

int
yfs_client::getdir(inum inum, dirinfo &din)
{
  int r = OK;
  lc->acquire(inum);

  printf("getdir %016llx\n", inum);
  extent_protocol::attr a;
  if (ec->getattr(inum, a) != extent_protocol::OK) {
    r = IOERR;
    goto release;
  }
  din.atime = a.atime;
  din.mtime = a.mtime;
  din.ctime = a.ctime;

 release:
  lc->release(inum);
  return r;
}

int
yfs_client::rpc_status_translate(extent_protocol::status ret)
{  
  if( ret == extent_protocol::RPCERR )
    return RPCERR;
  if( ret == extent_protocol::NOENT )
    return NOENT;
  if(ret == extent_protocol::IOERR )
    return IOERR;
  return OK;
}

int
yfs_client::read(inum inum, std::string &s)
{
  return rpc_status_translate( ec->get(inum, s) );
}

int
yfs_client::write(inum inum, std::string s)
{
  return rpc_status_translate( ec->put(inum, s) );
}

int
yfs_client::remove(inum inum)
{
  return rpc_status_translate( ec->remove(inum) );
}

int
yfs_client::lookup(inum parent, std::string name, inum &ino)
{
  if( !isdir(parent) ) 
    return NOENT;

  lc->acquire(parent);
  std::string pdir;
  int ret = read(parent, pdir);
  if( ret != OK )
    goto release;
  
  ino = find_in_dir(name, pdir);
  if( ino ) {
    ret = EXIST;
  }
  else {
    ret = NOENT;
  }
 release:
  lc->release(parent);
  return ret;
}

int
yfs_client::create(inum parent, std::string name, inum &ino, bool dir)
{
  if( !isdir(parent) ) 
    return NOENT;

  bool acq_file_lock = false;
  lc->acquire(parent);
  
  std::string pdir;
  int ret = read(parent, pdir);
  if( ret != OK )
    goto release;

  // check for existence
  if( find_in_dir(name, pdir) ) {
    ret = EXIST;
    goto release;
  }

  if(dir) {
    ino = (rand() & 0x000000007FFFFFFF);
  } else {
    ino = (rand() & 0x00000000FFFFFFFF) | 0x0000000080000000;
  }
    
  //// make a new ino
  //while(1) {
  //  extent_protocol::attr a;
  //  lc->acquire(ino);
  //  acq_file_lock = true;
  //  ret = ec->getattr(ino, a);
  //  if( ret == extent_protocol::NOENT )
  //    break;
  //  else if( ret != extent_protocol::OK ) {
  //    ret = RPCERR;
  //    goto release;
  //  }
  //  lc->release(ino);
  //  acq_file_lock = false;
  //}
  printf("create %016llx\n", ino);

  // write file
  ret = write(ino, "");
  if( ret != OK ) {
    goto release;
  }

  // write directory
  ret = write(parent, add_to_dir(name, ino, pdir));

 release:
  if(acq_file_lock)
    lc->release(ino);
  lc->release(parent);
  return ret;
}

int
yfs_client::unlink(inum parent, std::string name)
{
  if( !isdir(parent) )
    return NOENT;

  bool acq_file_lock = false;
  lc->acquire(parent);
  std::string pdir;
  int ret = read(parent, pdir);
  inum ino;
  if( ret != OK )
    goto release;
  
  // check for existence
  if( ! (ino = find_in_dir(name, pdir)) ) {
    ret = NOENT;
    goto release;
  }

  printf("unlink %016llx\n", ino);

  lc->acquire(ino);
  acq_file_lock = true;
  // remove from extent server
  ret = remove(ino);
  if( ret != OK )
    goto release;
  
  // write directory
  ret = write(parent, remove_from_dir(name, pdir));
 release:
  if(acq_file_lock)
    lc->release(ino);
  lc->release(parent);
  return ret;
}

int
yfs_client::readdir(inum dir, std::vector<dirent> &entries)
{
  if( !isdir(dir) )
    return NOENT;

  lc->acquire(dir);
  std::string dstr;
  const char *d;
  int ret = read(dir, dstr);
  if( ret != OK )
    goto release;

  entries.clear();
  d = dstr.c_str();
  while( *d != NULL ) {
    dirent entry;
    entry.name = std::string(d);
    d += strlen(d) + 1; // skip filename
    entry.inum = n2i(std::string(d));
    d += strlen(d) + 1; // skip inum
    entries.push_back(entry);
  }

  ret = OK;
 release:
  lc->release(dir);
  return ret;
}

int
yfs_client::resize(inum ino, unsigned long long size)
{
  if( !isfile(ino) )
    return IOERR;

  lc->acquire(ino);
  std::string s;
  int ret = read(ino, s);
  if( ret != OK )
    goto release;

  if( size > s.size() ) {
    std::ostringstream ost;
    ost << s << std::string(size - s.size(), NULL);
    ret = write(ino, ost.str());
  }
  else {
    ret = write(ino, s.substr(0, size));
  }
 release:
  lc->release(ino);
  return ret;
}

int
yfs_client::read_part(inum ino, size_t sz, size_t off, std::string &s)
{
  lc->acquire(ino);
  int ret = read(ino, s);
  if( ret != OK )
    goto release;

  if( off >= s.size() ) {
    s = "";
  }
  else {
    s = s.substr(off, sz);
  }

  ret = OK;
 release:
  lc->release(ino);
  return ret;
}

int
yfs_client::write_part(inum ino, size_t off, std::string s)
{
  lc->acquire(ino);
  std::string file;
  int ret = read(ino, file);
  if(ret != OK )
    goto release;

  if(off >= file.size()) {
    file.append(off - file.size() + 1, NULL);
  }
  file.replace(off, s.size(), s);

  ret = write(ino, file);
 release:
  lc->release(ino);
  return ret;
}

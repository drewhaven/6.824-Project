// yfs client.  implements FS operations using extent and lock server
#include "yfs_client.h"
#include "extent_client.h"
#include "lock_client.h"
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
  srand(time(NULL));
  
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
int
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
  // You modify this function for Lab 3
  // - hold and release the file lock

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

  return r;
}

int
yfs_client::getdir(inum inum, dirinfo &din)
{
  int r = OK;
  // You modify this function for Lab 3
  // - hold and release the directory lock

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
  return r;
}

int
yfs_client::read(inum inum, std::string &s)
{
  int ret = ec->get(inum, s);
  if( ret == extent_protocol::RPCERR )
    return RPCERR;
  if( ret == extent_protocol::NOENT )
    return NOENT;
  if(ret == extent_protocol::IOERR )
    return IOERR;
  return OK;
}

int
yfs_client::write(inum inum, std::string s)
{
  int ret = ec->put(inum, s);
  if( ret == extent_protocol::RPCERR )
    return RPCERR;
  if( ret == extent_protocol::NOENT )
    return NOENT;
  if(ret == extent_protocol::IOERR )
    return IOERR;
  return OK;
}

int
yfs_client::lookup(inum parent, std::string name, inum &ino)
{
  if( !isdir(parent) ) 
    return NOENT;

  std::string pdir;
  int ret = read(parent, pdir);
  if( ret != OK )
    return ret;
  
  ino = find_in_dir(name, pdir);
  if( ino )
    return EXIST;

  return NOENT;
}

int
yfs_client::create(inum parent, std::string name, inum &ino)
{
  if( !isdir(parent) ) 
    return NOENT;

  std::string pdir;
  int ret = read(parent, pdir);
  if( ret != OK )
    return ret;

  // check for existence
  if( find_in_dir(name, pdir) )
    return EXIST;

  // make a new ino
  while(1) {
    ino = (rand() & 0x00000000FFFFFFFF) | 0x0000000080000000;
    extent_protocol::attr a;
    ret = ec->getattr(ino, a);
    if( ret == extent_protocol::NOENT )
      break;
    else if( ret != extent_protocol::OK )
      return IOERR;
  }
  printf("create %016llx\n", ino);

  // write file
  ret = write(ino, "");
  if( ret != OK ) {
    return ret;
  }

  // write directory
  ret = write(parent, add_to_dir(name, ino, pdir));
  return ret;
}

int
yfs_client::readdir(inum dir, std::vector<dirent> &entries)
{
  if( !isdir(dir) )
    return NOENT;

  std::string dstr;
  int ret = read(dir, dstr);
  if( ret != OK )
    return ret;

  entries.clear();
  const char *d = dstr.c_str();
  dirent entry;
  while( *d != NULL ) {
    entry.name = std::string(d);
    d += strlen(d) + 1; // skip filename
    entry.inum = n2i(std::string(d));
    d += strlen(d) + 1; // skip inum
    entries.push_back(entry);
  }

  return OK;
}

int
yfs_client::resize(inum ino, unsigned long long size)
{
  if( !isfile(ino) )
    return IOERR;

  std::string s;
  int ret = read(ino, s);
  if( ret != OK )
    return ret;

  if( size > s.size() ) {
    std::ostringstream ost;
    ost << s << std::string(size - s.size(), NULL);
    ret = write(ino, ost.str());
  }
  else {
    ret = write(ino, s.substr(0, size));
  }
  return ret;
}

int
yfs_client::read_part(inum ino, size_t sz, size_t off, std::string &s)
{
  int ret = read(ino, s);
  if( ret != OK )
    return ret;

  if( off >= s.size() ) {
    s = "";
  }
  else {
    s = s.substr(off, sz);
  }

  return OK;
}

int
yfs_client::write_part(inum ino, size_t off, std::string s)
{
  std::string file;
  int ret = read(ino, file);
  if(ret != OK )
    return ret;

  if(off >= file.size()) {
    file.append(off - file.size() + 1, NULL);
  }
  file.replace(off, s.size(), s);

  ret = write(ino, file);
  return ret;
}

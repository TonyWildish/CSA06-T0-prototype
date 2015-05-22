#include "T0/Repacker/interface/InputEventFile.h"
#include "T0/Repacker/interface/Error.h"
#include "Utilities/StorageFactory/interface/StorageFactory.h"
#include "SealBase/Storage.h"
#include "SealBase/StringFormat.h"

namespace T0Repack {
  
  InputEventFile::InputEventFile(const std::string & fURL) :
    EventFile(fURL),m_size(0) {
    seal::IOOffset    size = -1;
    // open file using seal::storage
    if (StorageFactory::get ()->check(fileURL().c_str(), &size))
      try {
	m_storage.reset(StorageFactory::get ()->open (fileURL().c_str(),seal::IOFlags::OpenRead));
      } catch(seal::Error & ce) {
	throw T0Repack::Error(std::string("T0Repack::InputEventFile: error in opening file ") +
			      fileURL(),&ce);
      }
    else throw 
	   T0Repack::Error(std::string("T0Repack::InputEventFile: file ")+
			   fileURL()+std::string(" does not exists"));
    
  }
  
  InputEventFile::~InputEventFile(){
    // protect in case of throw in constructor
    if(m_storage) m_storage->close();
  }
  
  seal::IOBuffer InputEventFile::get(seal::IOOffset location, seal::IOSize bufferSize) {
    seal::IOSize  n = getBuffer(internalBuffer, location, bufferSize);
    return seal::IOBuffer(&internalBuffer[0],n);
  }

  seal::IOSize InputEventFile::getBuffer(std::vector<char> & vbuf, seal::IOOffset location, seal::IOSize bufferSize) {
    // local buffer (not deleted for efficiency)
    if (bufferSize>vbuf.size()) vbuf.resize(bufferSize);
    char * buf = &vbuf[0];
    seal::IOSize  n = 0;
    try {
      m_storage->position(location);
      n = m_storage->read (buf, bufferSize);
    } catch(seal::Error & ce) {
      throw T0Repack::Error(std::string("T0Repack::InputEventFile: error in reading file ") +
			    fileURL(),&ce);
    }
    
    if (n!= bufferSize) 
      throw T0Repack::Error(seal::StringFormat ("Error in reading from file %1. Asked for %2; got %3")
			    .arg (fileURL()).arg (int(bufferSize)).arg (int(n)));
    
    m_size+=n;
    
    return n;
  }
  
  
  
}


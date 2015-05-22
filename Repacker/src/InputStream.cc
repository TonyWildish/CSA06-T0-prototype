#include "T0/Repacker/interface/InputStream.h"
#include "T0/Repacker/interface/Error.h"
#include "SealBase/StringFormat.h"
#include "Utilities/StorageFactory/interface/StorageFactory.h"
#include "SealBase/Storage.h"
#include <memory>

namespace T0Repack {


  InputStream::InputStream(const std::string & fURL): 
    m_fileURL(fURL){readIt();}

  void InputStream::readIt()
  {
    seal::IOOffset    size = -1;
    std::auto_ptr<seal::Storage> s;
    // open file using seal::storage
    if (StorageFactory::get ()->check(fileURL().c_str(), &size))
      try {
	s.reset(StorageFactory::get ()->open (fileURL().c_str(),seal::IOFlags::OpenRead));
      } catch(seal::Error & ce) {
	throw T0Repack::Error(std::string("T0Repack::InputStream: error in opening file ") +
			      fileURL(),&ce);
      }
    else throw 
	   T0Repack::Error(std::string("T0Repack::InputStream: file ")+
			   fileURL()+std::string(" does not exists"));
    
    // read the full file in a single buffer and load it into itself
    std::vector<char> lbuf(size+1,'\0');
    seal::IOSize nn = 0;
    try {
      nn = s->read(&lbuf[0],size);
    } catch (seal::Error & ce) {
      throw T0Repack::Error(std::string("T0Repack::InputStream: error in  reading from  file ") +
			    fileURL(),&ce);
    }      
    if (int(nn)!=size) {
      throw T0Repack::Error(seal::StringFormat ("Error in reading from file %1. Asked for %2; got %3").arg (fileURL()).arg (int(size)).arg (int(nn)));
    }
    this->str(&lbuf[0]);
    s->close();    
  }
    
  
}

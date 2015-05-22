#include "T0/Repacker/interface/PosixCRC.h"
#include "SealZip/Adler32.h"
#include "SealZip/CRC32.h"
#include "SealIOTools/StorageInputStream.h"
#include "SealBase/Signal.h"
#include "SealBase/DebugAids.h"
#include "SealBase/Signal.h"
#include <iostream>
#include<vector>
#include <string>
# include <boost/shared_ptr.hpp>

#include "T0/Repacker/interface/Error.h"
#include "PluginManager/PluginManager.h"
#include "Utilities/StorageFactory/interface/StorageFactory.h"
#include "SealBase/Storage.h"
#include "SealBase/StringFormat.h"

using namespace seal;
int main(int argc, char **argv)
{
  Signal::handleFatal (argv [0]);
  PluginManager::get ()->initialise ();
  
  try {
    
    if (argc < 1)
      {
	std::cerr << "please give file name" <<std::endl;
	return EXIT_FAILURE;
      }
    
    boost::shared_ptr<seal::Storage> storage;
    seal::IOOffset m_size=0;
    std::vector<unsigned char> vbuf(1000000);
    
    seal::IOOffset    size = -1;
    // open file using seal::storage
    if (StorageFactory::get ()->check(argv [1], &size))
      try {
	storage.reset(StorageFactory::get ()->open (argv [1],seal::IOFlags::OpenRead));
      } catch(seal::Error & ce) {
	throw T0Repack::Error(std::string("t0cksum: error in opening file ") +
			      argv [1] ,&ce);
      }
    else throw 
	   T0Repack::Error(std::string("t0cksum: file ")+
			   argv[1]+std::string(" does not exists"));
    
    
    //    std::cerr << "file " << argv[1] << "of size " << size << " opened" << std::endl;
    
    // CRC32  checksum;
    // Adler32 checksum;
    PosixCRC checksum;
    IOSize read;
    
    while ((read = storage.get()->read (&vbuf[0], vbuf.size()))) {
      m_size+=read;
      checksum.update(&vbuf[0],read);
    }

    if (m_size!=size) 
      throw T0Repack::Error(seal::StringFormat ("Error in reading file %1. size %2; got %3")
			    .arg (argv[1]).arg (int(size)).arg (int(m_size)));

    // same output of /bin/cksum
    std::cout << checksum.value () << " " << m_size << " " << argv[1]
	      << std::endl;
    
  }
  catch(seal::Error & ce) {    
    std::cerr << ce.explain() << std::endl;
    return EXIT_FAILURE;
  }
  
  return EXIT_SUCCESS;
}


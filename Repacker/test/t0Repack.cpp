#include "Utilities/StorageFactory/interface/StorageFactory.h"
#include "Utilities/StorageFactory/interface/StorageAccount.h"
#include "PluginManager/PluginManager.h"
#include "SealBase/Storage.h"
#include "SealIOTools/StorageStreamBuf.h"
#include "SealBase/DebugAids.h"
#include "SealBase/Signal.h"
#include "SealBase/TimeInfo.h"
#include <iostream>
#include <cmath>
#include <limits>
#include <string>
#include <vector>
#include <queue>
#include <fstream>
#include <sstream>
#include <memory>
# include <boost/shared_ptr.hpp>
# include "SealBase/IOError.h"
#include "T0/Repacker/interface/OutputEventFile.h"
#include "T0/Repacker/interface/Error.h"
#include "T0/Repacker/interface/Repacker.h"



using namespace seal;
int main (int argc, char **argv)
{

  TimeInfo::init ();

  Signal::handleFatal (argv [0]);
  PluginManager::get ()->initialise ();
  
  if (argc < 4)
    {
      std::cerr << "please give dataset number, output file name, and list of index files" <<std::endl;
      return EXIT_FAILURE;
    }
  
  StorageFactory::get ()->enableAccounting(true);
  
  int datasetN = ::atol(argv [1]);
  std::string outputURL = argv[2];
  std::cerr << "write to file " << outputURL
	    << " dataset " << datasetN << std::endl;
  
  T0Repack::Repacker repacker;
  repacker.selectStream(datasetN);
  IOSize totSize=0;
  IOSize totIN=0;
  
  try {
    
    repacker.openOutput(outputURL);
    
    // parse index file
    // read buffer
    // select and copy to output file
    
    for (int i=3; i<argc;i++) {
      std::cerr << "reading from index file " <<  argv[i] << std::endl;
      repacker.parseIndex(argv[i]);
      
      // totIN+=inputEventFile.size();
      
    }
    
  }
  catch(seal::Error & ce) {
    
    std::cerr << ce.explain() << std::endl;
    return EXIT_FAILURE;
  }
  
  const T0Repack::OutputEventFile * outputFile = repacker.file(outputURL);
  
  std::cerr << "Read a total of " << totIN << " bytes" << std::endl;
  std::cerr << "copied a total of " << totSize << " bytes" << std::endl;
  std::cerr << "Output file size " << outputFile->size() << ", checksum " 
	    << outputFile->checksum() << std::endl;
  
  std::cerr << "stats:\n" << StorageAccount::summaryText () << std::endl;
  
  return EXIT_SUCCESS;
}



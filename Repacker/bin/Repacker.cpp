#include "T0/Repacker/interface/Sequencer.h"
#include "T0/Repacker/interface/Error.h"
#include "PluginManager/PluginManager.h"
#include "SealBase/DebugAids.h"
#include "SealBase/Signal.h"
#include "SealBase/TimeInfo.h"
#include <iostream>




using namespace seal;
int main (int argc, char **argv)
{

  TimeInfo::init ();

  Signal::handleFatal (argv [0]);
  PluginManager::get ()->initialise ();
  
  if (argc < 2)
    {
      std::cerr << "please give config file name" <<std::endl;
      return EXIT_FAILURE;
    }
  
  T0Repack::Sequencer sequencer;

  try {
    sequencer.parse(argv[1]);
    sequencer.execute();
  }
  catch(seal::Error & ce) {
    
    std::cerr << ce.explain() << std::endl;
    return EXIT_FAILURE;
  }
  
  return EXIT_SUCCESS;

}

#include "PluginManager/PluginManager.h"
#include "T0/Repacker/interface/Sequencer.h"
#include "T0/Repacker/interface/Error.h"
#include <iostream>
#include "SealBase/DebugAids.h"
#include "SealBase/Signal.h"
#include <iostream>




using namespace seal;
int main (int argc, char **argv)
{

  Signal::handleFatal (argv [0]);
  PluginManager::get ()->initialise ();
  
  if (argc < 2)
    {
      std::cerr << "please give config file name" <<std::endl;
      return EXIT_FAILURE;
    }
  
  try {
    T0Repack::Sequencer sequencer(argv[1]);
  }
  catch(seal::Error & ce) {    
    std::cerr << ce.explain() << std::endl;
    return EXIT_FAILURE;
  }
  
  
  return EXIT_SUCCESS;

}

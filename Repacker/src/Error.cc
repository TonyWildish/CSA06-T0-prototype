#include "T0/Repacker/interface/Error.h"
//#include "SealBase/StringFormat.h"

namespace T0Repack {

  Error::Error (const std::string&context, seal::Error *chain /* = 0 */)
    : seal::IOError (context.c_str(), chain ? (*chain).clone() : 0)
  {}

  Error::Error (const char *context, seal::Error *chain /* = 0 */)
    : seal::IOError (context, chain ? (*chain).clone() : 0)
  {}
  
  
  seal::Error *
  Error::clone (void) const
  { return new Error (*this); }
  
  void
  Error::rethrow (void)
  { throw *this; }

}

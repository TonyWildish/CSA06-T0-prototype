#ifndef T0Repack_ERROR_H
# define T0Repack_ERROR_H

# include "SealBase/IOError.h"
# include <string>

namespace T0Repack {

  class
  Error : public seal::IOError
  {
  public:
    explicit Error (const std::string& context, seal::Error *chain=0);
    explicit Error (const char *context, seal::Error *chain=0);
    
    virtual seal::Error *clone (void) const;
    virtual void	rethrow (void);
    
  private:
    
  };

}

#endif //  T0Repack_ERROR_H

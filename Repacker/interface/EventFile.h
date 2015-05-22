#ifndef T0Repack_EventFile_H
# define T0Repack_EventFile_H


#include "SealBase/IOBuffer.h"

#include <string>

namespace T0Repack {
  
  
  class EventFile {
  public:
    explicit EventFile(const std::string & fURL);
    virtual ~EventFile();
    inline const std::string & fileURL() const { return  m_fileURL;}
  private:
    std::string m_fileURL;
  };
  
}

#endif  // T0Repack_EventFile_H

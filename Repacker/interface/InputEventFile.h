#ifndef T0Repack_InputEventFile_H
# define T0Repack_InputEventFile_H

#include "T0/Repacker/interface/EventFile.h"
#include "SealBase/Storage.h"
#include <string>
# include <boost/shared_ptr.hpp>
#include<vector>

namespace T0Repack {


  class InputEventFile : public EventFile {
  public:
    // construct and open (throws on error, clean before throwing ...)
    explicit InputEventFile(const std::string & fURL);
    // close and clean
    virtual ~InputEventFile();
    // random read (use external storage)
    seal::IOSize getBuffer(std::vector<char> & vbuf, 
			   seal::IOOffset location, seal::IOSize bufferSize);
    // random read
    seal::IOBuffer get(seal::IOOffset location, seal::IOSize bufferSize);
    // total amount of byte read
    inline seal::IOOffset size() const { return m_size;}
    //
    
    // for some tests...
    inline seal::Storage & storage() const { return *m_storage;}

  private:
    boost::shared_ptr<seal::Storage> m_storage;
    seal::IOOffset m_size;
    std::vector<char> internalBuffer;
  };
  
}

#endif  // T0Repack_InputEventFile_H

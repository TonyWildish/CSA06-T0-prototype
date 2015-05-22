#ifndef T0Repack_OutputEventFile_H
# define T0Repack_OutputEventFile_H

#include "T0/Repacker/interface/EventFile.h"
#include "SealBase/Storage.h"
#include <string>
#include <boost/shared_ptr.hpp>
#include "SealZip/Digest.h"
# include "SealZip/Checksum.h"

namespace T0Repack {


  class OutputEventFile : public EventFile {
  public:
    // construct and open (throws on error, clean before throwing ...)
    explicit OutputEventFile(const std::string & fURL);
    // close and clean
    virtual ~OutputEventFile();
    // sequential write
    void put(const seal::IOBuffer & event);
    // total amount of byte written
    inline seal::IOOffset size() const { return m_size;}

    // enable checksum (return previous state)
    bool enableChecksum(bool enable = true);
    // return checksum
    unsigned checksum() const;

    // enable md5 (return previous state)
    bool enableMD5(bool enable = true);
    // return md5 (binary)
    seal::Digest::Value md5() const;
    // return checksum (as string)
    std::string md5Formatted() const;

  private:
    void updateMD5(const seal::IOBuffer & event);
    void updateCRC32(const seal::IOBuffer & event);

  private:
    boost::shared_ptr<seal::Digest> m_md5;
    boost::shared_ptr<seal::Checksum> m_crc32;
    boost::shared_ptr<seal::Storage> storage;
    seal::IOOffset m_size;
  };
		
}

#endif  // T0Repack_OutputEventFile_H

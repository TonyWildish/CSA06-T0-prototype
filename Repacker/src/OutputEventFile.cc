#include "T0/Repacker/interface/OutputEventFile.h"
#include "T0/Repacker/interface/Error.h"
#include "SealZip/MD5Digest.h"
#include "SealZip/CRC32.h"
#include "T0/Repacker/interface/PosixCRC.h"
#include "Utilities/StorageFactory/interface/StorageFactory.h"
#include "SealBase/Storage.h"
#include "SealBase/StringFormat.h"


namespace T0Repack {

  OutputEventFile::OutputEventFile(const std::string & fURL) :
    EventFile(fURL),m_size(0)
  {
    try {
      storage.reset(StorageFactory::get ()->open (fileURL(),
						  seal::IOFlags::OpenWrite
						  | seal::IOFlags::OpenCreate
						  | seal::IOFlags::OpenTruncate));
    } catch(seal::Error & ce) {
      throw T0Repack::Error(std::string("T0Repack::OutputEventFile: error in opening file ") +
			    fileURL(),&ce);
    }
  }
  
  
  OutputEventFile::~OutputEventFile() {
    // protect in case of error in constructor
    if(storage) storage->close();
  }
  
  void OutputEventFile::put(const seal::IOBuffer & event) {
    try {
      storage->write(event);
    } catch(seal::Error & ce) {
      throw T0Repack::Error(std::string("T0Repack::OutputEventFile: error in writing file ") +
			    fileURL(),&ce);
    }
    
    updateCRC32(event);
    updateMD5(event);
    m_size+=event.size();
    
  }
  
  bool OutputEventFile::enableMD5(bool enable /* = true */) {
    bool curr = m_md5.get();
    if (enable&&!curr) m_md5.reset(new seal::MD5Digest);
    if (!enable) m_md5.reset();
    return curr;
  }
  
  seal::Digest::Value OutputEventFile::md5() const {
    return m_md5.get() ?  m_md5->digest() : seal::Digest::Value();
  }
  
  std::string OutputEventFile::md5Formatted() const {
    return m_md5.get() ?  m_md5-> format() : std::string();
  }
  
  void OutputEventFile::updateMD5(const seal::IOBuffer & event) {
    if (m_md5.get()) m_md5->update(event);
  }
  
  bool OutputEventFile::enableChecksum(bool enable /* = true */) {
    bool curr = m_crc32.get();
    if (enable&&!curr) m_crc32.reset(new seal::PosixCRC);
    if (!enable) m_crc32.reset();
    return curr;
  }
  
  unsigned  OutputEventFile::checksum() const {
    return m_crc32.get() ?  m_crc32->value() : 0;
  }
  
  void OutputEventFile::updateCRC32(const seal::IOBuffer & event) {
    if (m_crc32.get()) m_crc32->update(event);
  }
  
  
}

#include "T0/Repacker/interface/FakeLoadFilter.h"

#include "SealBase/TimeInfo.h"
#include "SealBase/IOBuffer.h"

namespace T0Repack {
  
  
  FakeLoadFilter::FakeLoadFilter(double inap) : m_nap(inap) {}

  bool  FakeLoadFilter::operator()(seal::IOBuffer) const {
    seal::TimeInfo::sleep(m_nap);
    return true;
  }
  
}

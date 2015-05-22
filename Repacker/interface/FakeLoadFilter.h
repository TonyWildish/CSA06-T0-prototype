#ifndef T0Repack_FakeLoadFilter_h
# define T0Repack_FakeLoadFilter_h
#include "T0/Repacker/interface/EventFilter.h"

namespace T0Repack {

  /** A Filter that fakes the elapsed real time
   * of an event consumer, not the cpu load... 
   * it just sleep "nap" sec...
   */
  class FakeLoadFilter : public EventFilter {
  public:
    explicit FakeLoadFilter(double inap);

    // sleep, return true
    virtual bool operator()(seal::IOBuffer) const;

  private:
    double m_nap;
  };

}

#endif // T0Repack_FakeLoadFilter_h

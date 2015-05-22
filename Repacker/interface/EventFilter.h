#ifndef T0Repack_EventFilter_h
# define T0Repack_EventFilter_h

namespace seal {
  class IOBuffer;
}

namespace T0Repack {

  /** plugin to the Repacker to 
   *  filter events according their content
   *  Abstract Base Class...
   */
  class EventFilter {
  public:
    virtual ~EventFilter(){}
    virtual bool operator()(seal::IOBuffer event) const =0;

  };

}

#endif // T0Repack_EventFilter_h

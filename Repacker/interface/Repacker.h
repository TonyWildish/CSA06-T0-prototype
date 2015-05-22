#ifndef T0Repack_Repacker_H
# define T0Repack_Repacker_H

#include "SealBase/IOBuffer.h"
#include<string>
#include <vector>
#include <queue>
# include <boost/shared_ptr.hpp>
#include<iosfwd>

namespace T0Repack {

  class EventFilter;

  class OutputEventFile;

  class RepackerThreads;

  class Repacker {
  public:
    Repacker();

    // close all files
    ~Repacker();

    void openOutput(const std::string & furl);
    void closeOutput(const std::string & furl);
    void selectStream(int streamN);
    void setTrace(int level);
    void threadsOff();
    void preseekOff();
    seal::IOSize  parseIndex(const std::string & indexFileName);

    inline int selectedStream() const { return m_selectedStream;}
    inline int traceLevel() const { return m_traceLevel;}

    void closeAllOutput();

    void setFilter(EventFilter * filter);

    const OutputEventFile * file(const std::string & furl) const;

    std::string summary() const;

  private:

    double elapsed() const;

    std::ostream & cout(int level=-9) const;
    
    bool filter(seal::IOBuffer event) const;


  private:

    double m_startTime;

    int m_traceLevel;
    int m_selectedStream;
    bool m_useThreads;
    bool m_usePreseek;

    typedef std::vector<boost::shared_ptr<OutputEventFile> > Output;
    Output m_output;
    boost::shared_ptr<EventFilter> m_filter;
    boost::shared_ptr<std::ostringstream>  m_summary;

  private:
    /* Multi thread implementation */
    friend class RepackerThreads;
  };


}




#endif  // T0Repack_Repacker_H

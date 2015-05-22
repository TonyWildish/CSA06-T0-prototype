#include "Utilities/StorageFactory/interface/StorageFactory.h"
#include "Utilities/StorageFactory/interface/StorageAccount.h"
#include "SealBase/TimeInfo.h"

#include "T0/Repacker/interface/Repacker.h"
#include "T0/Repacker/src/RepackerThread.h"

#include "T0/Repacker/interface/Error.h"
#include "T0/Repacker/interface/InputStream.h"
#include "T0/Repacker/interface/InputEventFile.h"
#include "T0/Repacker/interface/OutputEventFile.h"
#include "T0/Repacker/interface/EventFilter.h"
#include<iostream>
#include<sstream>



#include "Utilities/StorageFactory/interface/StorageAccountProxy.h"
#include<vector>
// rfio data structure
extern "C" {
struct iovec64 {
        off64_t iov_base ;
        int iov_len ;
};
}
typedef std::vector<iovec64> IOVec;
inline void push(IOVec& vec, off64_t b, int l) {
  vec.push_back(iovec64());
  vec.back().iov_base = b;
  vec.back().iov_len = l;
} 


namespace T0Repack {

  double  Repacker::elapsed() const {
    return seal::TimeInfo::realNsecs ()- m_startTime;
  }


  std::ostream & Repacker::cout(int level /*=-9 */) const {
    static std::ostream cnull(0);
    return level <= m_traceLevel ? 
      std::cerr << "Repacker " << elapsed() *1e-6 << "ms" << ": "  : 
      cnull;
  }

  std::string Repacker::summary() const { 
    return 
      StorageAccount::summaryText (true) + std::string("\n\n") +(*m_summary).str();
  }

  Repacker::Repacker() : 
    m_startTime (seal::TimeInfo::realNsecs ()),
    m_traceLevel(0),
    m_selectedStream(-1),
    m_useThreads(true),
    m_usePreseek(true),
    m_summary(new std::ostringstream())
  {    
    StorageFactory::get ()->enableAccounting(true);
  }

  Repacker::~Repacker() {
    closeAllOutput();
  }

  void Repacker::openOutput(const std::string & furl) {
    cout(1) << "opening output file " << furl << std::endl;
    m_output.push_back(Output::value_type(new OutputEventFile(furl)));
    m_output.back().get()->enableChecksum();
  }

  void Repacker::closeOutput(const std::string & furl) {
    for(Output::iterator i=m_output.begin(); 
	i!=m_output.end();i++) {
      if ( (*i).get()->fileURL()==furl) {
	cout(1) << "closing output file "<< furl 
	       << "; wrote " << (*i).get()->size() << " bytes, checksum " 
	       << (*i).get()->checksum() << std::endl;
	(*m_summary) << "wrote " << (*i).get()->size() << " bytes to "
		     << (*i).get()->fileURL() << " , checksum " 
		     << (*i).get()->checksum() << std::endl;
	m_output.erase(i);
	break;
      }
    }
  }

  void Repacker::closeAllOutput() {
    for(Output::iterator i=m_output.begin(); 
	i!=m_output.end();i++) {
      cout(1) << "closing output file "<< (*i).get()->fileURL()
	     << "; wrote " << (*i).get()->size() << " bytes, checksum " 
	     << (*i).get()->checksum() << std::endl;
      (*m_summary) << "wrote " << (*i).get()->size() << " bytes to "
		   << (*i).get()->fileURL() << " , checksum " 
		   << (*i).get()->checksum() << std::endl;
      (*i).reset();
    }
    m_output.clear();
  }



  const OutputEventFile * Repacker::file(const std::string & furl) const {
    for(Output::const_iterator i=m_output.begin(); 
	i!=m_output.end();i++) {
      if ( (*i).get()->fileURL()==furl) {
	return (*i).get();
      }
    }
    return 0;
  }
  

  void Repacker::selectStream(int streamN) {
    cout(1) << "set stream to " << streamN  << std::endl;
    m_selectedStream = streamN;
  }

  void Repacker::setTrace(int level) {
    cout(1) << "set trace level to " << level  << std::endl;
    m_traceLevel=level;
  }

  void Repacker::threadsOff() {
    cout(1) << "switching threads off"  << std::endl;
    m_useThreads = false;
  }

  void Repacker::preseekOff() {
    cout(1) << "switching preseek off"  << std::endl;
    m_usePreseek = false;
  }

  void Repacker::setFilter(EventFilter * filter) {
    m_filter.reset(filter);
  }

  bool Repacker::filter(seal::IOBuffer event) const {
    cout(3) << " filtering " << std::endl;
    if (!m_filter) return true;
    return (*m_filter)(event);
  }

  seal::IOSize Repacker::parseIndex(const std::string & indexFileName) {
    seal::IOOffset totIN=0;
    cout(1) << "reading from index file " <<  indexFileName << std::endl;

    boost::shared_ptr<RepackerThreads> threadImpl;
 
    if (m_useThreads) threadImpl.reset(new RepackerThreads(this));

    T0Repack::InputStream indexFiles(indexFileName);
    
    std::istream & in = indexFiles;
    std::string line1; std::getline(in, line1);
    cout(3) << "first line is:\n" << line1 << std::endl;
    std::string::size_type pos = line1.find('=');
    if (pos!=std::string::npos) pos = line1.find_first_not_of(' ',pos+1);
    if (pos==std::string::npos)
      throw T0Repack::Error(std::string("Repacker::parseIndex : badly formed index file ") +  
			    indexFileName + std::string("first line is:\n")  + line1);
    line1.erase(0,pos);
    cout(1) << "input event file is " << line1 << std::endl;
    T0Repack::InputEventFile inputEventFile(line1);
    if (m_useThreads) {
      // FIXME, most probably enough to construct the TheadImpl with these param
      threadImpl->addInput(inputEventFile);
      // not needed... (can be implemented above...)
      threadImpl->addOutputs(m_output);
    }
    IOVec iov;
    try { 
      while(in) {
	int dataset=-1;
	seal::IOOffset bufLoc = -1;
	seal::IOSize   bufSize = 0;
	in >> dataset >> bufLoc >> bufSize;
	if (bufLoc<0||bufSize==0) {
	  if (in.eof())
	    cout(3) << "eof encountered"  << std::endl;
	  else
	    cout(3) << "read spurious record "
		    << dataset << " " << bufLoc << " " <<  bufSize << std::endl;
	  continue;
	}
	if (selectedStream()==-1 || dataset==selectedStream()) {
	  cout(3) << "copy buf at " << bufLoc << " of size " << bufSize << std::endl;
	  push(iov, bufLoc, bufSize);
	}
      } // while
      

      if (m_usePreseek) {
	StorageAccountProxy * rf = 
	  dynamic_cast<StorageAccountProxy*>(&inputEventFile.storage());
	if (rf) (*rf).preseek(iov);
	else std::cerr << "error not StorageAccountProxy  " << std::endl;
      }


      for (IOVec::const_iterator p=iov.begin(); p!=iov.end();p++) {
	if (m_useThreads) {
	  if ( threadImpl->copy((*p).iov_base,(*p).iov_len) ) {
	    totIN+=(*p).iov_len;
	  }
	} else {
	  seal::IOBuffer evBuffer;
	  // read
	  evBuffer = inputEventFile.get((*p).iov_base,(*p).iov_len);
	  // check filter
	  if (filter(evBuffer)) {
	    // copy to all output file
	    for(Output::iterator i=m_output.begin();i!=m_output.end();i++) {
	      (*i).get()->put(evBuffer);
	    }
	    totIN+=(*p).iov_len;
	  }
	}
      } // for
    }// try
    catch (seal::Error & ce) {
      // close threads before leaving...
      threadImpl.reset();
      throw;
    }

    // close threads
    threadImpl.reset();

    // FIXME this not anymore the requested in input...
    /// and for the thread is also wrong (previous buffer...)
    cout(3) << "Requested " <<  totIN 
	   << " bytes from file " << line1 << std::endl;

    cout(1) << "Read " << inputEventFile.size()  
	   << " bytes from file " << line1 << std::endl;

    return inputEventFile.size();

  }   


}
  
  
  

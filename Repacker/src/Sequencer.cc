#include "T0/Repacker/interface/Sequencer.h"
#include "T0/Repacker/interface/InputStream.h"
#include "T0/Repacker/interface/Error.h"
#include "SealBase/StringFormat.h"
#include <iostream>

namespace T0Repack {

  std::ostream & Sequencer::cout(int level /*=-9 */) {
    static std::ostream cnull(0);
    return level <= m_traceLevel ? std::cerr : cnull;
  }

  const std::string & Sequencer::actionName(int i) {
    static const std::string local[N_Actions] = 
      { "OpenFileURL",
	"CloseFileURL",
	"IndexFile",
	"SelectStream",
	"ThreadsOff",
	"PreseekOff",
	"TraceLevel",
	"INVALID"
      };
    return local[i];
  }
  
  Sequencer::Actions Sequencer::action(const std::string & name) {
    static const Actions local[N_Actions] = 
      {OPEN, CLOSE, INDEX, SELECT, NOTHREADS, NOPRESEEK, TRACE, INVALID};
    for (int i=0;i<N_Actions;i++)
      if (name==actionName(i)) return local[i];
    return INVALID;
    
  }
  
  Sequencer::Sequencer() 
    : m_traceLevel(0)
  {
  }

  Sequencer::~Sequencer()
  {
    cout() << std::endl
	   << m_repacker.summary() << std::endl;
  }

  Sequencer::Sequencer(const std::string& configFileName) : 
    m_traceLevel(0) 
  {
    parse(configFileName);
  }
  
  void Sequencer::parse(const std::string& configFileName) {
    bool ok=true;
    cout(2) << "start parsing config file " <<  configFileName << std::endl;
    T0Repack::InputStream config(configFileName);
    std::istream & in = config;
    int nline=0;
    while(in) {
      std::string line; std::getline(in, line);
      nline++;
      cout(2) << "cofig line " << nline << ": " << line << std::endl;
      if (line.empty()) continue;
      if (line[0]=='#') continue;

      if (!parseLine(line)) {
	ok=false;
	cout(-1) << "Invalid config syntax at line " << nline 
		 << " of file " << configFileName << "\n ==> |"
		 << line <<"|" << std::endl;
      }
    }
    cout(2) << "\nparsing config file " <<  configFileName 
	    << " completed \n\n" << std::endl;
    if (!ok) throw Error(std::string("Invalid config syntax in file ")+configFileName);
  }
  
  bool Sequencer::parseLine(const std::string& line) {
    std::string::size_type pos = line.find('=');
    std::string key(line,0,pos);
    if (pos!=std::string::npos) pos = line.find_first_not_of(' ',pos+1);
    if (pos==std::string::npos) return false; // FIXME should we throw???
    std::string value(line,pos);
    // clean key
    pos = key.find_first_not_of(" \t\b");
    if (pos!=std::string::npos) key.erase(0,pos);
    pos = key.find_first_of(" \t\b");
    if (pos!=std::string::npos) key.resize(pos);
    // identify action
    Actions  act = action(key);
    if (act==INVALID) {
      // FIXME should we throw???
      cout(-1) << "invalid action " << key << " in line\n" << line << std::endl;
      return false;
    }
    //clean value
    pos = value.find_first_of(" \t\b");
    if (pos!=std::string::npos) value.resize(pos);

    // handle trace
    if (act==TRACE) m_traceLevel = ::atol(value.c_str());

    m_steps.push(Step());
    m_steps.back().action=act;
    swap(m_steps.back().value,value);
    cout(3) << "pushed new step " 
	    << actionName(m_steps.back().action)
	    << ", " << m_steps.back().value << std::endl;

    return true;
  }
  
  
  void Sequencer::execute() {
    while(!m_steps.empty()) {
      switch(m_steps.front().action) {
      case OPEN :
	m_repacker.openOutput(m_steps.front().value);
	break;
      case CLOSE :
	m_repacker.closeOutput(m_steps.front().value);
	break;
      case INDEX :
	m_repacker.parseIndex(m_steps.front().value); 
	break;
      case SELECT: 
	m_repacker.selectStream(::atol(m_steps.front().value.c_str()));
	break;
      case NOTHREADS :
	if ((m_steps.front().value[0]!='f'))
	  m_repacker.threadsOff();
	break;
      case NOPRESEEK :
	if ((m_steps.front().value[0]!='f'))
	  m_repacker.preseekOff();
	break;
      case TRACE :
	m_repacker.setTrace(::atol(m_steps.front().value.c_str()));
	break;
      case INVALID :
	cout(-1) << "Invalid action?" << std::endl;
      }
      
      m_steps.pop();
    }
    

  }
  
   
}

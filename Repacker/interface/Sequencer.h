#ifndef T0Repack_Sequencer_H
# define T0Repack_Sequencer_H

#include<string>
#include "T0/Repacker/interface/Repacker.h"

namespace T0Repack {



  class Sequencer {
  public:
    Sequencer();
    ~Sequencer();
    explicit Sequencer(const std::string& configFileName);

    void parse(const std::string& configFileName);
    void execute();

    // for the prototype....
    inline Repacker & repacker() { return m_repacker;}

  private:
    enum {N_Actions=8};
    enum Actions {OPEN, CLOSE, INDEX, SELECT, NOTHREADS, NOPRESEEK, TRACE, INVALID}; 
    static const std::string & actionName(int i);
    static Actions action(const std::string & name);

    void oneStep();
    
    struct Step {
      inline Step() :action(INVALID){}
      Actions action;
      std::string value;
    };

    bool parseLine(const std::string& line);

  private:
    std::ostream & cout(int level=-9);


  private:
    int m_traceLevel;
    Repacker m_repacker;
    std::queue<Step> m_steps;
  };


}




#endif  // T0Repack_Sequencer_H

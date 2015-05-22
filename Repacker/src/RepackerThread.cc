#include "T0/Repacker/src/RepackerThread.h"
#include "T0/Repacker/interface/Error.h"


namespace {
  struct cond_predicate
  {
    cond_predicate(int& var, int val) : _var(var), _val(val) { }
    
    bool operator()() { return _var == _val; }
    
    int& _var;
    int _val;
  };
  
  class thread_adapter
  {
  public:
    thread_adapter(void (*func)(void*), void* param) : _func(func), _param(param) { }
    void operator()() const { _func(_param); }
  private:
    void (*_func)(void*);
    void* _param;
  };
}


namespace T0Repack {
  
  void RepackerThreads::writeThread(void * param) {
    RepackerThreads * repackerThreads = static_cast<RepackerThreads * >(param);
    OutputDropBox & dropBox = repackerThreads->outputBox;
    int me = dropBox.addWriter();
    OutputEventFile * writer = repackerThreads->repacker().m_output[me].get();    

    std::ostream & cout = repackerThreads->repacker().cout(2);
   
    cout << "start writing thread " << me << std::endl;
    
    while(dropBox.write(*writer,me));
    
    cout << "end writing thread " << me << std::endl;
    
  }
  
  void RepackerThreads::readThread(void * param) {
    RepackerThreads * repackerThreads = static_cast<RepackerThreads * >(param);
    InputDropBox & dropBox = repackerThreads->inputBox;
    
    InputEventFile & reader = repackerThreads->reader();    

    std::ostream & cout = repackerThreads->repacker().cout(2);

    cout << "start reading thread " << std::endl;
    
    while(dropBox.read(reader));
    
    cout << "end reading thread " << std::endl;
  
  }

  //------------------------------------------------------

  
  RepackerThreads::RepackerThreads(Repacker * parent) :
    m_parent(parent), m_reader(0){

    std::ostream & cout = repacker().cout(2);
    cout << "RepackerThreads constructed" << std::endl;

  }
  
  RepackerThreads::~RepackerThreads() {

    std::ostream & cout = repacker().cout(3);
    cout << "RepackerThreads start close-out" << std::endl;

    // flush last buffer
    if (copy(-1,0)) 
    // notify to end (if still active)
      inputBox.get(inbuf,-1,0);
    outputBox.put(outbuf,0);
    // waits threds have finished
    m_threads.join_all();

    cout << "RepackerThreads destructed" << std::endl;

  }
  
  // create a thread for the input file
  void RepackerThreads::addInput(InputEventFile & input) {
    m_reader = &input;
    m_threads.create_thread(thread_adapter(&readThread,this));
  }
  
  /* create one thread for each output file
   * a call back assign the index...
   */
  void  RepackerThreads::addOutputs(Repacker::Output & output) {
    for (int i=0; i<output.size(); i++)
      m_threads.create_thread(thread_adapter(&writeThread,this));
  }
  


  /*  push buffer to input thread, get previous read buffer
   *  swap it and push it to the writers  
   *
   */
  bool  RepackerThreads::copy(seal::IOOffset location, seal::IOSize bufferSize) 
  {
    // tell thread to read buffer, retrieve previous
    seal::IOSize n = inputBox.get(inbuf,location,bufferSize);
    std::ostream & cout = repacker().cout(3);
    cout << "RepackerThreads::copy got " << n << " bytes" << std::endl;
    // the first time buf is empty...
    if (n==0) return true;
    //free reading thread
    inbuf.swap(outbuf);
    // wait threads have finished to write
    // drop buffer in thread if filter ok...
    if (repacker().filter(seal::IOBuffer(&outbuf[0],n))) {
      outputBox.put(outbuf,n);
      return true;
    }
    return false;
  }
  

  // -----  OUTPUT ---------------------------------------------------  
  
  RepackerThreads::OutputDropBox::OutputDropBox()
    : outbuf(1000000), nout(0), ce(0), writing(0)
  {}
  
  void RepackerThreads::OutputDropBox::put(std::vector<char> & ibuf, seal::IOSize n) {
    // wait that all threads finish write to swap
    ScopedLock gl(lock);
    done.wait(gl, cond_predicate(writing, 0));
    bool err = false;
    // if error in previous write throw....
    if (ce==0) {
      outbuf.swap(ibuf);
      nout = n;
    }
    else {
      nout = 0;  // force thread to exit
      err = true;
    }
    undo(); // clean al bits, set writing to its size...
    //    buffer = ibuf;
    // notify threads buffer is ready
    doit.notify_all();
    if (err) throw T0Repack::Error("in output thread",ce);
  }
  
  
  bool RepackerThreads::OutputDropBox::write(OutputEventFile & writer, int it) {
    ScopedLock gl(lock);
    // wait if box empty or this thread already consumed...
    if (m_done[it]) doit.wait(gl);
    bool ret=true;
    // nout==0 notify thread to exit....
    if (nout==0) ret=false;
    else
      try {
	writer.put(seal::IOBuffer(&outbuf[0],nout));  
      } catch(seal::Error & lce) {
	ce = lce.clone();
      } 
    {
      // declare it finishes
      ScopedLock wl(wlock);
      m_done[it]=true;
      writing--;
    } 
    done.notify_all(); 
    return ret;
  }
  
  
  int RepackerThreads::OutputDropBox::addWriter() {
    ScopedLock wl(wlock);
    m_done.push_back(true);
    return m_done.size()-1;
  }
  
  void RepackerThreads::OutputDropBox::undo() {
    ScopedLock wl(wlock);
    writing= m_done.size();
    std::fill(m_done.begin(),m_done.end(),false);
  }


  // INPUT -----------------------------------------------

   RepackerThreads::InputDropBox::InputDropBox()  : 
     start(true), end(false),
     m_location(-1),m_bufferSize(0), 
     inbuf(1000000), nin(0), 
     ce(0)
   {
     
   }

  seal::IOSize 
  RepackerThreads::InputDropBox::get(std::vector<char> & ibuf, 
				     seal::IOOffset location, seal::IOSize bufferSize)
  {
    ScopedLock gl(lock);
     // if thread is over return...
    if (end) return 0;
    // wait that thread finish to read before swapping
    // not first time!
    if ( (!start) && nin==0) done.wait(gl);
    
    seal::IOSize ret = 0;
    // if error in previous read throw....
    if (ce==0) {
      inbuf.swap(ibuf);
      m_location = location;
      m_bufferSize = bufferSize;
      ret = nin;
    }
    else {
      m_location = -1;
      m_bufferSize = 0;
    }
    nin=0; 
    start=false;
    // notify threads buffer is ready
    doit.notify_all();
    if (ce) throw T0Repack::Error("in input thread",ce);
    return ret;
  }


  bool RepackerThreads::InputDropBox::read(InputEventFile & reader) 
  {
    ScopedLock gl(lock);
    // wait if box empty or this thread already consumed...
    if (start || nin!=0) doit.wait(gl);
    bool ret=true;
    // location ==-1 notify thread to exit....
    if (m_location<0)   { 
      end=true; 
      ret=false;
    }
    else
      try {
	// read
	nin = reader.getBuffer(inbuf, m_location, m_bufferSize);
	if (nin==0) {

	  end=true; 
	  ret=false; // stop thread
	}
      } catch(seal::Error & lce) {
	ce = lce.clone();
      } 
    
    done.notify_all(); 
    return ret;
  }

  
  
} // namespace T0Repack 

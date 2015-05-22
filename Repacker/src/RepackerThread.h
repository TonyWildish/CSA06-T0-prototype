#ifndef T0Repack_RepackerThread_H
# define T0Repack_RepackerThread_H
#include "T0/Repacker/interface/Repacker.h"
#include "T0/Repacker/interface/InputEventFile.h"
#include "T0/Repacker/interface/OutputEventFile.h"

# include "SealBase/IOError.h"

#include <boost/thread/thread.hpp>
#include <boost/thread/condition.hpp>
#include <boost/thread/mutex.hpp>
typedef boost::mutex::scoped_lock ScopedLock;
#include <vector>

namespace T0Repack {


  /** Multi-Thread implementation for the Repacker
   *
   */
  class RepackerThreads {
  public:

    explicit RepackerThreads(Repacker * parent);

    ~RepackerThreads();

    void addInput(InputEventFile & input);
    void addOutputs(Repacker::Output & output);

    bool copy(seal::IOOffset location, seal::IOSize bufferSize);

    
    static void writeThread(void * param);

    static void readThread(void * param);

  private:


    inline Repacker & repacker() { return *m_parent; }

    inline InputEventFile & reader() { return *m_reader;}

    class OutputDropBox {
    public:

      OutputDropBox();

      // sequential write (called by main)
      void put(std::vector<char> & ibuf, seal::IOSize n);

      // called by thread
      bool write(OutputEventFile & writer, int it);

      /* add a writer (called by thread itself)
	 return thread index....
      */
      int addWriter();

      // clear bits (declare box ready...)
      void undo();

      std::vector<bool> m_done;
      std::vector<char> outbuf;
      seal::IOSize nout;

      seal::Error * ce;
      int writing;

      // writing lock (FIXME needed???)
      mutable boost::mutex wlock;
      // swap lock
      mutable boost::mutex lock;

      mutable boost::condition doit;
      mutable boost::condition done;

    };

    class InputDropBox {
    public:
      
      InputDropBox();

      // random read (called by main)
      seal::IOSize get(std::vector<char> & ibuf, 
		 seal::IOOffset location, seal::IOSize bufferSize);

      // called by thread
      bool read(InputEventFile & reader);

      bool start;
      bool end;

      seal::IOOffset m_location; 
      seal::IOSize m_bufferSize;

      std::vector<char> inbuf;
      seal::IOSize nin;

      seal::Error * ce;

      // swap lock
      mutable boost::mutex lock;

      mutable boost::condition doit;
      mutable boost::condition done;
      
    };


  private:
    Repacker * m_parent;
    InputEventFile * m_reader;
    boost::thread_group m_threads;
    OutputDropBox outputBox;
    InputDropBox  inputBox;
    // local buffers
    std::vector<char>  inbuf;
    std::vector<char> outbuf;
    

  };


}

#endif //  T0Repack_RepackerThread

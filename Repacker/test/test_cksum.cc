#include "T0/Repacker/interface/PosixCRC.h"
#include "SealBase/PipeCmd.h"
#include "SealIOTools/StdInputStream.h"
#include "SealZip/CheckedInputStream.h"
#include <string> 
#include <iostream>
#include <fstream>
#include <sstream>
#include <vector>


using namespace seal;
int main(int argc, char * argv[]) {


  std::vector<char> buf(100000);
  IOSize read=0;

  std::ifstream f(argv[0]);

  StdInputStream  s(&f); 
  CheckedInputStream  t (&s, new PosixCRC());
  IOOffset tot=0;
  while ((read=t.read(&buf[0],buf.size()))) tot+=read;

  std::ostringstream  os;
  os << t.checksum ()->value ()
     << " " << tot << " " << argv[0]<< std::endl;

  std::cout << "||" << os.str() << "||"<< std::endl;

  std::string unixResult;
  PipeCmd cmd(std::string("cksum ")+argv[0],IOFlags::OpenRead);
  while ((read=cmd.read(&buf[0],buf.size())))
	 unixResult.append(&buf[0],read);

  std::cout << "||" << unixResult << "||"<< std::endl;

  if (os.str()!=unixResult) std::cerr << "not the same!" << std::endl;
  return 0;

}

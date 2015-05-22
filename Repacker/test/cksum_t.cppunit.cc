#include "T0/Repacker/interface/PosixCRC.h"
#include "SealBase/PipeCmd.h"
#include "SealIOTools/StdInputStream.h"
#include "SealZip/CheckedInputStream.h"
#include <string> 
#include <iostream>
#include <fstream>
#include <sstream>
#include <vector>

#include <cppunit/extensions/HelperMacros.h>

class testPosixCRC : public CppUnit::TestFixture {
  CPPUNIT_TEST_SUITE(testPosixCRC);
  CPPUNIT_TEST(checkAganstUnix);
  CPPUNIT_TEST_SUITE_END();
public:
  void setUp(){}
  void tearDown() {}
  void checkAganstUnix();
};

CPPUNIT_TEST_SUITE_REGISTRATION(testPosixCRC);


using namespace seal;
// int main(int argc, char * argv[]) {


void testPosixCRC::checkAganstUnix() {
  std::string progName;

#ifdef __linux__
 {
   std::ifstream cmd("/proc/self/cmdline");
   char c;
   while (cmd.get(c)) 
     if (c==0) break;
     else progName+=c;
 }
#endif // __linux__
  
  std::vector<char> buf(100000);
  IOSize read=0;
  
  std::ifstream f(progName.c_str());
  
  StdInputStream  s(&f); 
  CheckedInputStream  t (&s, new PosixCRC());
  IOOffset tot=0;
  while ((read=t.read(&buf[0],buf.size()))) tot+=read;
  
  std::ostringstream  os;
  os << t.checksum ()->value ()
     << " " << tot << " " << progName<< std::endl;
  
  std::cout << "\n||" << os.str() << "||"<< std::endl;
  
  std::string unixResult;
  PipeCmd cmd(std::string("cksum ")+progName,IOFlags::OpenRead);
  while ((read=cmd.read(&buf[0],buf.size())))
	 unixResult.append(&buf[0],read);

  std::cout << "||" << unixResult << "||"<< std::endl;

  if (os.str()!=unixResult) std::cerr << "not the same!" << std::endl;

  CPPUNIT_ASSERT (os.str()==unixResult);

}

#ifndef T0Repack_InputStream_H
# define T0Repack_InputStream_H
#include <sstream>
#include <string>

namespace T0Repack {
  /* read full file from seal:storage and save it in a string
   */
  class InputStream : public  std::istringstream {
  public:
    explicit  InputStream(const std::string & fURL);
    inline const std::string & fileURL() const { return  m_fileURL;}

  private:
    void readIt();
    std::string m_fileURL;		

  };

}
#endif //  T0Repack_InputStream_H

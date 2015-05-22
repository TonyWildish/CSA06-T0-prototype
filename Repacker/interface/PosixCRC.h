#ifndef SEAL_ZIP_PosixCRC_H
# define SEAL_ZIP_PosixCRC_H

//<<<<<< INCLUDES                                                       >>>>>>

# include "SealZip/Checksum.h"

namespace seal {
//<<<<<< PUBLIC DEFINES                                                 >>>>>>
//<<<<<< PUBLIC CONSTANTS                                               >>>>>>
//<<<<<< PUBLIC TYPES                                                   >>>>>>
//<<<<<< PUBLIC VARIABLES                                               >>>>>>
//<<<<<< PUBLIC FUNCTIONS                                               >>>>>>
//<<<<<< CLASS DECLARATIONS                                             >>>>>>

/** Compute a Posix CRC checksum of a data stream.  
 *
 * Based on the algorithm published by in the "Open Group Base Specification Iuuse 6"
 * IEEE Std 1003.1, 2004 Edition
 */
class PosixCRC : public Checksum
{
public:
    PosixCRC (void);
    // implicit copy constructor
    // implicit destructor
    // implicit copy constructor

    virtual unsigned	value (void) const;
    virtual void	update (const void *data, IOSize n);
    virtual void	reset (void);

private:
    unsigned 		m_value;	// Current raw checksum value
    IOOffset            m_size;         // total number of byte read
};

//<<<<<< INLINE PUBLIC FUNCTIONS                                        >>>>>>
//<<<<<< INLINE MEMBER FUNCTIONS                                        >>>>>>

} // namespace seal
#endif // SEAL_ZIP_PosixCRC_H

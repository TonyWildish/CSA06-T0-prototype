#!/usr/local/bin/perl -w
#
# Run a job as a daemon, saving output to a logfile...
#
# T.Wildish. 14/03/00
#

use strict;
use Symbol;
use Carp;
use POSIX;
use Getopt::Long;

my ($ChildSigSet,$logfile,$ppid,$pid);
my ($verbose,$cuttime,$help);

#====================================================================
sub usage
{

 croak <<EOF

  Usage: $0 {options} where options are:

  --verbose gives some output, otherwise it's silent
  --logfile is the name of the logfile. When it is cut or truncated, the
  `netlogger-format' date will be appended to the name.

  The `netlogger-format' time is of the form YYYYMMDDhhmmss, zero-padded.
You can force a value for one of the fields by giving the appropriate option,
--year, --month, --day, --hour, --minute, or --second. They default to now.

  You can cause a new logfile to be started by sending this script a `HUP'
 signal.

EOF
;
}

#====================================================================
sub stamp
{
  my ($year,$month,$day,$hour,$minute,$seconds) = @_;

  my @n = localtime;

  defined($year)    or $year    = $n[5] + 1900;
  defined($month)   or $month   = $n[4] + 1;
  defined($day)     or $day     = $n[3];
  defined($hour)    or $hour    = $n[2];
  defined($minute)  or $minute  = $n[1];
  defined($seconds) or $seconds = $n[0];

  sprintf("%04d%02d%02d%02d%02d%02d",
                  $year,$month,$day,$hour,$minute,$seconds);
}

#====================================================================
sub cutlog
{
  my $stamp = stamp;
  $verbose && print "$stamp: Truncating logfile\n";
  close STDOUT;
  rename $logfile, $logfile . '.' . $stamp || 
    croak "Cannot rename $logfile: $!\n";
  open STDOUT, ">>$logfile" or croak "Cannot open $logfile: $!\n";
  open(STDERR,">&STDOUT") or croak "Cannot dup STDOUT: $!\n";
  select(STDOUT); $|=1;
  $verbose && print 
	"$stamp Truncated logfile saved as $logfile.$stamp\n" ,
        "Use 'kill $pid' to kill the child.\n" ;
#       "Use 'kill -HUP $ppid' to truncate the logfile.\n";
  $SIG{HUP} = \&cutlog;
}

#====================================================================
my ($year,$month,$day,$hour,$minute,$seconds);
$verbose = 0;
GetOptions( "verbose"   => \$verbose,
            "logfile=s" => \$logfile,
	    "year=i"    => \$year,
            "month=i"   => \$month,
            "day=i"     => \$day,
            "hour=i"    => \$hour,
            "minute=i"  => \$minute,
            "seconds=i" => \$seconds,
            "help"      => \$help
          );

$logfile || usage;
$help && usage;

#====================================================================
# Fork, disconnect from terminal etc
$ChildSigSet  = POSIX::SigSet->new(SIGCHLD);

# Block signals while forking...
if ( ! defined(sigprocmask(SIG_BLOCK, $ChildSigSet)) )
{ croak "Couldn't block SIGCHLD: $!\n"; }

croak "fork: $!\n" unless defined($ppid=fork);

# Can unblock signals now...
if ( ! defined(sigprocmask(SIG_UNBLOCK, $ChildSigSet)) )
{ croak "Couldn't unblock SIGCHLD: $!\n"; }

#====================================================================
# Parent has nothing to do now that it is disconnected, so it exits.
if ( $ppid )
{
  $verbose && print "Use 'kill -HUP $ppid' to truncate the logfile.\n";
  exit 0;
}

#====================================================================
# Manipulate output streams etc, declare myself to be a session leader.
open STDOUT, ">>$logfile" or croak "Cannot open $logfile: $!\n";
open(STDERR,">&STDOUT") or croak "Cannot dup STDOUT: $!\n";
select(STDOUT); $|=1;
open (STDIN,">>/dev/null") or carp "Couldn't redirect STDIN: $!\n";

POSIX::setsid;

#====================================================================
$SIG{INT} = $SIG{PIPE} = 'IGNORE';
$SIG{HUP} = \&cutlog;

#====================================================================
$verbose && print 'Process ',$$,': ', join(' ',@ARGV), "\n";

$pid = open(CHILD,"-|");
if ( $pid )
{
  $verbose && print "Use 'kill $pid' to kill the child.\n";
  while ( <CHILD> ) { print; }
  close CHILD || carp "Child exited...? $?\n";
}
else
{
  exec join(' ',@ARGV) || croak "Can't exec @ARGV: $!\n";
}

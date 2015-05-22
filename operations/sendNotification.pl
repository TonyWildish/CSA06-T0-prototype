#!/usr/bin/perl -w

use strict;
use POE::Session;
use POE::Kernel;
use T0::Logger::Sender;
use Getopt::Long;

my ($help,$config);
my ($runnumber,$lumisection,$instance,$count,$stoptime,$filename,$pathname);
my ($hostname,$destination,$setuplabel,$stream,$status,$type,$safety,$nevents);
my ($filesize,$starttime,$checksum,$index,$appname,$appversion,$hltkey);
my $sender;

sub usage
{
  die <<EOF;

  Usage $0 <options>

  ...no detailed help yet, sorry...

EOF
}

$help = 0;
GetOptions(
	   "help"          => \$help,
	   "config=s"      => \$config,
	   "DESTINATION=s" => \$destination,
	   "FILENAME=s"    => \$filename,
	   "PATHNAME=s"    => \$pathname,
	   "HOSTNAME=s"    => \$hostname,
	   "FILESIZE=i"    => \$filesize,
	   "TYPE=s"        => \$type,
	   "SETUPLABEL=s"  => \$setuplabel,
	   "STREAM=s"      => \$stream,
	   "RUNNUMBER=i"   => \$runnumber,
	   "LUMISECTION=i" => \$lumisection,
	   "NEVENTS=i"     => \$nevents,
	   "APP_NAME=s"    => \$appname,
	   "APP_VERSION=s" => \$appversion,
	   "HLTKEY=s"      => \$hltkey,
	   "INDEX=s"       => \$index,
	   "START_TIME=i"  => \$starttime,
	   "STOP_TIME=i"   => \$stoptime,
	   "CHECKSUM=s"    => \$checksum,
          );

defined($config)      or die "Need \"config\" argument...\n";

defined($destination) or die "Need \"DESTINATION\" argument...\n";
defined($filename)    or die "Need \"FILENAME\" argument...\n";
defined($pathname)    or die "Need \"PATHNAME\" argument...\n";
defined($hostname)    or die "Need \"HOSTNAME\" argument...\n";
defined($filesize)    or die "Need \"FILESIZE\" argument...\n";
defined($type)        or die "Need \"TYPE\" argument...\n";

#defined($setuplabel)  or die "Need \"SETUPLABEL\" argument...\n";
#defined($stream)      or die "Need \"STREAM\" argument...\n";
#defined($runnumber)   or die "Need \"RUNNUMBER\" argument...\n";
#defined($lumisection) or die "Need \"LUMISECTION\" argument...\n";
#defined($nevents)     or die "Need \"NEVENTS\" argument...\n";
#defined($appname)     or die "Need \"APP_NAME\" argument...\n";
#defined($appversion)  or die "Need \"APP_VERSION\" argument...\n";
#defined($hltkey)      or die "Need \"HLTKEY\" argument...\n";
#defined($index)       or die "Need \"INDEX\" argument...\n";

defined($starttime) or $starttime = 0;
defined($stoptime)  or $stoptime = 0;
defined($checksum)  or $checksum = "0";


$help && usage;

POE::Session->create(
                     inline_states => {
                                       _start => \&start_tasks,
                                      },
                     args => [ ],
                    );

POE::Kernel->run();
exit;

sub start_tasks
  {
    my ( $kernel, $heap ) = @_[ KERNEL, HEAP ];

    $sender = T0::Logger::Sender->new
      (
       Name            => 'StorageManagerInterface',
       Config          => $config,
       RetryInterval   => 0,
       Verbose         => 0,
       Debug           => 0,
       Quiet           => 0,
       OnError         => sub { return 0; },
       OnConnect       => \&OnConnect,
      );
  }

sub OnConnect
  {
    my ( $kernel, $heap ) = @_[ KERNEL, HEAP ];

    my %loghash1 = (
		    DAQFileClosed      => 1,
		    T0FirstKnownTime   => time,
		    DESTINATION        => $destination,
		    FILENAME           => $filename,
		    PATHNAME           => $pathname,
		    HOSTNAME           => $hostname,
		    FILESIZE           => $filesize,
		    TYPE               => $type,
		    SETUPLABEL         => $setuplabel,
		    STREAM             => $stream,
		    RUNNUMBER          => $runnumber,
		    LUMISECTION        => $lumisection,
		    NEVENTS            => $nevents,
		    APP_NAME           => $appname,
		    APP_VERSION        => $appversion,
		    HLTKEY             => $hltkey,
		    INDEX              => $index,
		    START_TIME         => $starttime,
		    STOP_TIME          => $stoptime,
		    CHECKSUM           => $checksum,
		   );
    $sender->Send( \%loghash1 );

    my %loghash2 = (
		    TransferStatus => '1',
		    STATUS => 'new',
		    FILENAME => $filename,
		   );
    $sender->Send( \%loghash2 );


    $kernel->yield('shutdown');
  }

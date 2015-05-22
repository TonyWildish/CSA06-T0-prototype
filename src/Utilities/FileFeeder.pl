#!/usr/bin/perl -w

use strict;
use POE;
use T0::Logger::Sender;
use T0::Index::Generator;
use T0::FileWatcher;
use T0::Iterator::Iterator;
use T0::Util;
use Getopt::Long;

my ($help,$verbose,$debug,$quiet);
my ($name,$config,$channel,$directory,$lfnlist);
my ($maxfiles,$interval,$rate);
my $restart;
my ($senderWatcher, $sender); #Let iterator to kill the watcher of the sender when there are no more files to feed.

#select STDERR; $| = 1;	# make unbuffered
#select STDOUT; $| = 1;	# make unbuffered

sub usage
{
  die <<EOF;

  Usage $0 <options>

  ...no detailed help yet, sorry...

EOF
}

$help = $verbose = $debug = 0;
$interval = 1.0;
$name = 'File::Feeder';
$config = "../Config/JulyPrototype.conf";
$channel = undef;
$directory = undef;
$lfnlist = undef;
GetOptions(     "help"          => \$help,
                "verbose"       => \$verbose,
                "quiet"         => \$quiet,
                "debug"         => \$debug,
		"name=s"	=> \$name,
                "config=s"      => \$config,
		"channel=s"     => \$channel,
		"directory=s"	=> \$directory,
		"lfnlist=s"     => \$lfnlist,
		"interval=f",	=> \$interval,
		"maxfiles=i"	=> \$maxfiles,
		"restart"       => \$restart,
          );
$help && usage;

defined($name) or die "Need \"name\" argument...\n";

POE::Session->create(
		     inline_states => {
				       _start => \&start_tasks,
				      },
		     args => [ ],
		    );

POE::Kernel->run();

exit;

sub start_tasks {
  my ( $kernel, $heap ) = @_[ KERNEL, HEAP ];

  $sender = T0::Logger::Sender->new
    (
     Name	     => 'File::Feeder::Sender',
     Config 	     => $config,
     RetryInterval   => 0,
     Verbose	     => $verbose,
     Debug  	     => $debug,
     Quiet   	     => $quiet,
     OnError	     => sub { return 0; },
     OnConnect       => \&OnConnect,
     QueueEntries    => 1,
     RetryInterval   => 3,
    );

  $senderWatcher = T0::FileWatcher->new
    (
     File            => $config,
     Object          => $sender,
     Interval        => $interval,
     Verbose         => $verbose,
     Debug           => $debug,
     Quiet           => $quiet,
    );

  Print $sender->Name, ", running on ", $sender->Host, ". My PID is ", $$, "\n";
}

sub OnConnect {
  my ( $session ) = $_[ SESSION ];

  T0::Iterator::Iterator->new(
			      Name	      => $name,
			      Config	      => $config,
			      Directory       => $directory,
			      LFNList         => $lfnlist,
			      Interval        => $interval,
			      MaxFiles        => $maxfiles,
			      Channel         => $channel,
			      Restart         => $restart,
			      CallbackSession => $session,
			      CallbackMethod  => 'send',
			      Sender          => $sender,
			      SenderWatcher   => $senderWatcher,
			      Verbose	      => $verbose,
			      Debug  	      => $debug,
			      Quiet   	      => $quiet,
			     );

  return 0;
}

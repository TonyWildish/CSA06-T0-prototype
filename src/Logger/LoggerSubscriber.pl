#!/usr/bin/perl -w

use strict;
use POE;
use Getopt::Long;
use T0::Logger::Sender;
use T0::Logger::Receiver;
use T0::Util;

my ($help,$verbose,$debug,$quiet);
my ($config,$retry,$host,$port,$key,$value,$unsubscribe);
my ($remotehost,$remoteport,$qentries);
my ($sender,$listener);

sub usage
{
  die <<EOF;

  Usage $0 <options>

  ...no detailed help yet, sorry...

EOF
}

$help = $verbose = $debug = $qentries = 0;
$retry = 3;
$host  = $remotehost = 'localhost';
$port  = 1027;
$key   = 'AnyKey';
$value = '.*';
$remoteport = 12346;
$unsubscribe = 0;
$config = "../Config/JulyPrototype.conf";
GetOptions(     "help"          => \$help,
                "verbose"       => \$verbose,
                "quiet"         => \$quiet,
                "debug"         => \$debug,
                "retry"         => \$retry,
                "config=s"      => \$config,
		"unsubscribe"	=> \$unsubscribe,
                "host=s"      	=> \$host,
                "port=i"      	=> \$port,
                "key=s"      	=> \$key,
                "value=s"      	=> \$value,
		"qentries"	=> \$qentries,
          );
$help && usage;

sub subscribe
{
  my %h;
  $h{RPC}   = $unsubscribe ? 'Unsubscribe' : 'Subscribe';
  $h{Host}  = $listener->Host;
  $h{Port}  = $listener->Port;
  $h{Key}   = $key;
  $h{Value} = $value;
  $h{RetryInterval} = $unsubscribe ? 0 : $retry;
  $h{QueueEntries}  = $unsubscribe ? 0 : 1;

  $sender->Send( \%h );
}

sub OnConnect
{
  my ( $self, $heap, $kernel ) = @_[ OBJECT, HEAP, KERNEL ];
  print "OnConnect: self=$self, heap=$heap\n";

# I could make this idempotent, but that isn't necessary, the subscription
# mechanism takes care of that for me...
  subscribe;
  return 0;
}

sub OnInput
{
  my ( $self, $heap, $input ) = @_[ OBJECT, HEAP, ARG0 ];
  print "OnInput: input=$input, i=",$input->{i},"\n";

# Check this was really something I want
  return 0 if ( !exists($input->{$key}) || $input->{$key} !~ m%$value% );

  print "This is for me...\n";

  return 1;
}

$sender = T0::Logger::Sender->new(
                Verbose		=> $verbose,
                Debug  		=> $debug,
                Quiet   	=> $quiet,
		RetryInterval	=> $retry,
		Receiver => { Host => $remotehost,
			      Port => $remoteport,
			    },
 		OnConnect	=> \&OnConnect,
 		OnError		=> sub { return 0; },
	);

$listener = T0::Logger::Receiver->new (
		Verbose		=> $verbose,
		Debug		=> $debug,
		Quiet		=> $quiet,
		Port		=> $port,
 		OnInput		=> \&OnInput,
	);

Print $sender->Name,", running on ",$sender->Host,". My PID is ",$$,"\n";
Print $listener->Name,", running on ",$listener->Host,':',$listener->Port,"\n";

POE::Kernel->run();
exit;

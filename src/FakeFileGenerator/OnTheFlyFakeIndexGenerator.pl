#!/usr/bin/perl -w

use strict;
use POE;
use T0::Logger::Sender;
use T0::Logger::Receiver;
use T0::FileWatcher;
use T0::Index::Generator;
use T0::Util;
use Getopt::Long;

my ($help,$verbose,$debug,$quiet);
my ($config,$retry,$key,$value,$unsubscribe);
my ($qentries);
my ($sender,$listener,$generator,$watcher,$interval);

sub usage
{
  die <<EOF;

  Usage $0 <options>

  ...no detailed help yet, sorry...

EOF
}

$help = $verbose = $debug = $qentries = 0;
$retry = 3;
$key   = 'InputReady';
$value = '.*raw$';
$unsubscribe = 0;
$config = "../Config/JulyPrototype.conf";
$interval = 3;
GetOptions(     "help"          => \$help,
                "verbose"       => \$verbose,
                "quiet"         => \$quiet,
                "debug"         => \$debug,
                "config=s"      => \$config,
		"unsubscribe"	=> \$unsubscribe,
                "key=s"      	=> \$key,
                "value=s"      	=> \$value,
                "interval=i"    => \$interval,
		"qentries"	=> \$qentries,
          );
$help && usage;

sub subscribe
{
  my %h;
  $h{RPC}   = $unsubscribe ? 'Unsubscribe' : 'Subscribe';
  $h{Host}  = 'localhost';
  $h{Port}  = $listener->Port;
  $h{Key}   = $key;
  $h{Value} = $value;
  $h{RetryInterval} = $unsubscribe ? 0 : $retry;
  $h{QueueEntries}  = $unsubscribe ? 0 : $qentries;

  print join(' ',%h),"\n";
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
  $self->Debug("OnInput: input=$input, = ",join(' ',%$input),"\n");

# Check this was really something I want
  return 0 if ( !exists($input->{$key}) || $input->{$key} !~ m%$value% );

  my %text;
  $text{$key} = $generator->Generate( file => $input->{$key},
                        	      size => $input->{Size},
                        	      protocol => 'rfio',
                      			 );
  $text{Host} = $generator->{Host},

  $sender->Send(\%text);
  return 1;
}

$sender = T0::Logger::Sender->new
	(
                Config  => $config,
                Verbose		=> $verbose,
                Debug  		=> $debug,
                Quiet   	=> $quiet,
 		OnConnect	=> \&OnConnect,
 		OnError		=> sub { return 0; },
	);

$listener = T0::Logger::Receiver->new
	(
		Name		=> 'Index::Receiver',
                Config  	=> $config,
		Verbose		=> $verbose,
		Debug		=> $debug,
		Quiet		=> $quiet,
 		OnInput		=> \&OnInput,
	);

$generator = T0::Index::Generator->new
        (
                Config  => $config,
                Verbose => $verbose,
                Debug   => $debug,
                Quiet   => $quiet,
        );

$watcher = T0::FileWatcher->new (
		File		=> $config,
		Object		=> $generator,
		Interval	=> $interval,
		Verbose		=> $verbose,
		Debug		=> $debug,
		Quiet		=> $quiet,
	);

$watcher->Interval($generator->ConfigRefresh);

Print $sender->Name,  ", running on ",$sender->Host,". My PID is ",$$,"\n";
Print $listener->Name,", running on ",$listener->Host,':',$listener->Port,"\n";

POE::Kernel->run();
exit;

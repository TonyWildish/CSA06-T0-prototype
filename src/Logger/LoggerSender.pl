#!/usr/bin/perl -w

use strict;
use POE;
use Getopt::Long;
use T0::Logger::Sender;
use T0::Util;

my ($help,$verbose,$debug,$quiet);
my ($config,$retry);

sub usage
{
  die <<EOF;

  Usage $0 <options>

  ...no detailed help yet, sorry...

EOF
}

$help = $verbose = $debug = 0;
$retry = 3;
$config = "../Config/JulyPrototype.conf";
GetOptions(     "help"          => \$help,
                "verbose"       => \$verbose,
                "quiet"         => \$quiet,
                "debug"         => \$debug,
                "retry"         => \$retry,
                "config=s"      => \$config,
          );
$help && usage;

sub OnConnect
{
  my ( $self, $heap, $kernel ) = @_[ OBJECT, HEAP, KERNEL ];

# Only start the timer first time round, or there will be one timer per time
# the receiver is restarted
  return 0 if $heap->{count};
  print "OnConnect: self=$self, heap=$heap\n";
  $kernel->state( 'timer', \&timer );
  $kernel->yield( 'timer' );
  return 0;
}

my $sender = T0::Logger::Sender->new(
		Config		=> $config,
                Verbose		=> $verbose,
                Debug  		=> $debug,
                Quiet   	=> $quiet,
		RetryInterval	=> $retry,
		OnConnect	=> \&OnConnect,
	);

Print "I am \"",$sender->Name,"\", running on ",$sender->Host,". My PID is ",$$,"\n";

sub Log { $sender->Send(@_); }

sub timer
{
  my ( $heap, $kernel ) = @_[ HEAP, KERNEL ];
  my %h = ( Host => 'localhost', AnyKey => 'AnyValue' );
  $heap->{count}++;
  print "Sending $heap->{count}\n";
  $sender->Send( scalar(localtime) . " $heap->{count}" );
  $h{'i'} = $heap->{count};
# $h{'j'} = [ $heap->{count}+1, $heap->{count}+3 ];
# $h{'k'} = { 'one' => 1, 'two' => 2 };
  Log( \%h );
  $kernel->delay_set( 'timer', 0.1 );
}

POE::Kernel->run();
exit;

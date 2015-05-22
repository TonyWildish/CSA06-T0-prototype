#!/usr/bin/perl -w

use strict;
use POE;
use T0::Logger::Sender;
use T0::Logger::Receiver;
use T0::Util;
use Getopt::Long;
use Data::Dumper;

my ($help,$verbose,$debug,$quiet);
my ($config,$retry,$key,$value,$unsubscribe);
my ($qentries);
my ($sender,$listener);

sub usage
{
  die <<EOF;

  Usage $0 <options>

  ...no detailed help yet, sorry...

EOF
}

#$Data::Dumper::Terse++;

$help = $verbose = $debug = $qentries = 0;
$retry = 0;
$key   = 'status';
$value = '^[1-9]';
$unsubscribe = 0;
$config = "../Config/JulyPrototype.conf";
GetOptions(     "help"          => \$help,
                "verbose"       => \$verbose,
                "quiet"         => \$quiet,
                "debug"         => \$debug,
                "config=s"      => \$config,
		"unsubscribe"	=> \$unsubscribe,
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
  $h{QueueEntries}  = $unsubscribe ? 0 : $qentries;

  print join(' ',%h),"\n";
  $sender->Send( \%h );
}

sub OnConnect
{
  my ( $self, $heap, $kernel ) = @_[ OBJECT, HEAP, KERNEL ];
  print "OnConnect: self=$self, heap=$heap\n";

  subscribe;
  return 0;
}

sub OnInput
{
  my ( $self, $heap, $input ) = @_[ OBJECT, HEAP, ARG0 ];
  $self->Debug("OnInput: input=$input, = ",join(' ',%$input),"\n");

# Check this was really something I want
  return 0 if ( !exists($input->{$key}) || $input->{$key} !~ m%$value% );

  my $a = Data::Dumper->Dump([$input]);
# $a =~ s%\n%%g;
# $a =~ s%\s\s+% %g;
  Print "--------------------------\n";
  print $a,"\n\n";

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
		Name		=> 'Error::Receiver',
                Config  	=> $config,
		Verbose		=> $verbose,
		Debug		=> $debug,
		Quiet		=> $quiet,
 		OnInput		=> \&OnInput,
	);

Print $sender->Name,  ", running on ",$sender->Host,". My PID is ",$$,"\n";
Print $listener->Name,", running on ",$listener->Host,':',$listener->Port,"\n";

POE::Kernel->run();
exit;

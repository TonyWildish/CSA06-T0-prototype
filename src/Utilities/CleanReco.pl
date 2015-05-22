#!/usr/bin/perl -w

use strict;
use POE;
use Getopt::Long;
use Sys::Hostname;
use File::Basename;
use T0::Logger::Sender;
use T0::Logger::Receiver;
use T0::Util;

my ($help,$verbose,$debug,$quiet);
my ($config,$retry,$key,$value);
my ($sender,$listener);
my $debug_me=1;

sub usage
{
  die <<EOF;

  Usage $0 <options>

  ...no detailed help yet, sorry...

EOF
}

$help = $verbose = $debug = 0;
$retry = 7;
$key   = 'RecoReady';
$value = '.*(raw|root)$';
$config = "../Config/JulyPrototype.conf";
GetOptions(     "help"          => \$help,
                "verbose"       => \$verbose,
                "quiet"         => \$quiet,
                "debug"         => \$debug,
                "retry"         => \$retry,
                "config=s"      => \$config,
                "key=s"      	=> \$key,
                "value=s"      	=> \$value,
          );
$help && usage;

sub subscribe
{
  my %h;
  $h{RPC}   = 'Subscribe';
  $h{Host}  = hostname();
  $h{Port}  = $listener->Port;
  $h{Key}   = $key;
  $h{Value} = $value;
  $h{RetryInterval} = $retry;
  $h{QueueEntries}  = 1;

  $sender->Send( \%h );
}

sub OnConnect
{
  my ( $self, $heap, $kernel ) = @_[ OBJECT, HEAP, KERNEL ];

# I could make this idempotent, but that isn't necessary, the subscription
# mechanism takes care of that for me...
  subscribe;
  return 0;
}

sub OnInput
{
  my ( $self, $kernel, $heap, $input ) = @_[ OBJECT, KERNEL, HEAP, ARG0 ];

# Check this was really something I want
  return 0 if ( !exists($input->{$key}) || $input->{$key} !~ m%$value% );

  $self->Verbose("OnInput: input=$input, = ",join(' ',%$input),"\n");

  my $dir = dirname $input->{RecoReady};
  $dir =~ s%/\.$%%;
  Print "Delete $dir\n";
  open RFRM, "yes 2>/dev/null | rfrm -r $dir |" or die "rfrm $dir: $!\n";
  while ( <RFRM> )
  {
    chomp;
    $verbose && print $_,"\n";
  }
  close RFRM;

  return 1;
}

$sender = T0::Logger::Sender->new
	(
		Name		=> 'CleanReco::Sender',
		Config		=> $config,
                Verbose		=> $verbose,
                Debug  		=> $debug,
                Quiet   	=> $quiet,
 		OnConnect	=> \&OnConnect,
 		OnError		=> sub { return 0; },
	);

$listener = T0::Logger::Receiver->new
	(
		Name		=> 'CleanReco::Receiver',
		Config		=> $config,
		Verbose		=> $verbose,
		Debug		=> $debug,
		Quiet		=> $quiet,
 		OnInput		=> \&OnInput,
	);

Print $sender->Name,", running on ",$sender->Host,". My PID is ",$$,"\n";
Print $listener->Name,", running on ",$listener->Host,':',$listener->Port,"\n";

POE::Kernel->run();
exit;

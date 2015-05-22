#!/usr/bin/perl -w

use strict;
use POE;
use Getopt::Long;
use T0::Logger::Sender;
use T0::Logger::Receiver;
use T0::Copy::Manager;
use T0::Util;

my ($help,$verbose,$debug,$quiet);
my ($config,$retry,$key,$value,$unsubscribe);
my ($copier,$sender,$listener,$generator);

select STDERR; $| = 1;	# make unbuffered
select STDOUT; $| = 1;	# make unbuffered

sub usage
{
  die <<EOF;

  Usage $0 <options>

  ...no detailed help yet, sorry...

EOF
}

$help = $verbose = $debug = 0;
$retry = 3;
$unsubscribe = 0;
$config = "../Config/JulyPrototype.conf";
GetOptions(     "help"          => \$help,
                "verbose"       => \$verbose,
                "quiet"         => \$quiet,
                "debug"         => \$debug,
                "retry"         => \$retry,
                "config=s"      => \$config,
		"unsubscribe"	=> \$unsubscribe,
                "key=s"      	=> \$key,
		"value=s"       => \$value,
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

  Print "Subscribing to key=\"$key\" with value=\"$value\"\n";
  $sender->Send( \%h );

  my %k;
  $k{RPC}   = $unsubscribe ? 'Unsubscribe' : 'Subscribe';
  $k{Host}  = $listener->Host;
  $k{Port}  = $listener->Port;
  $k{Key}   = 'SetState';
  $k{Value} = '.*';
  $k{RetryInterval} = $unsubscribe ? 0 : $retry;
  $k{QueueEntries}  = $unsubscribe ? 0 : 1;

  $sender->Send( \%k );
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

# Deal with state transitions. These aren't postedthrough the POE kernel,
# they may need more urgent response than that, so we call the routine
# instead.
  if ( exists($input->{SetState}) )
    {
      $kernel->post($copier->{Name}, 'SetState', $input);
      return 0;
    }

# Check this was really something I want
  return 0 if ( !exists($input->{$key}) || $input->{$key} !~ m%$value% );

  $self->Verbose("OnInput: input=$input, = ",join(' ',%$input),"\n");

  delete $input->{$key};
  $kernel->post( $copier->{Name}, 'process_file', $input );

  return 1;
}

$sender = T0::Logger::Sender->new
	(
		Config		=> $config,
                Verbose		=> $verbose,
                Debug  		=> $debug,
                Quiet   	=> $quiet,
 		OnConnect	=> \&OnConnect,
 		OnError		=> sub { return 0; },
	);

$listener = T0::Logger::Receiver->new
	(
		Config		=> $config,
		Name		=> 'Copy::Receiver',
		Verbose		=> $verbose,
		Debug		=> $debug,
		Quiet		=> $quiet,
 		OnInput		=> \&OnInput,
	);

$copier = T0::Copy::Manager->new
	(
		Name		=> 'Copy::Manager',
		Config		=> $config,
                Verbose		=> $verbose,
                Debug		=> $debug,
                Quiet		=> $quiet,
		Logger		=> $sender,
		SelectTarget    => \&SelectTarget,
        );

$key   = $key   || $copier->{InputKey} || 'NewFileReadyForCopy';
$value = $value || $copier->{Value}    || '1';

Print $sender->Name,", running on ",$sender->Host,". My PID is ",$$,"\n";
Print $listener->Name,", running on ",$listener->Host,':',$listener->Port,"\n";

POE::Kernel->run();
exit;

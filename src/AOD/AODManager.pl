#!/usr/bin/perl -w

use strict;
use POE;
use Getopt::Long;
use T0::Logger::Sender;
use T0::Logger::Receiver;
use T0::Component::Manager;
use T0::Util;

my ($help,$verbose,$debug,$quiet);
my ($config,$retry,$skey,$key,$value);
my ($reconstructor,$sender,$listener,$generator);

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
                "key=s"      	=> \$key,
                "value=s"      	=> \$value,
          );
$help && usage;

sub subscribe
{
  my %h;
  $h{RPC}   = 'Subscribe';
  $h{Host}  = $listener->Host;
  $h{Port}  = $listener->Port;
  $h{Key}   = $key;
  $h{Value} = $value;
  $h{RetryInterval} = $retry;
  $h{QueueEntries}  = 1;

  Print "Subscribing to key=\"$key\" with value=\"$value\"\n";
  $sender->Send( \%h );

  my %f = %h;
  $f{Key}   = $skey;
  $f{Value} = '.*';
  $sender->Send( \%f );

  my %g = %h;
  $g{Key}   = 'SetState';
  $g{Value} = '.*';
  $sender->Send( \%g );
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
    $kernel->post($reconstructor->Name, 'SetState', $input);
    return 0;
  }

# Check this was really something I want
  $input->{$key} = delete $input->{$skey} if defined $input->{$skey};
  if ( ! ( exists($input->{$key}) && $input->{$key} =~ m%$value% ) )
  {
    return 0;
  }

  $self->Verbose("OnInput: input=$input, = ",join(' ',%$input),"\n");

  my $p = $reconstructor->{Partners}->{Worker};
  if ( $p )
  {
    no strict 'refs';
    my $t = ${$p}{DataType};
    return 1 unless IsRequiredSource(
				    $t,
				    $input->{Parent}->{Channel}
				    );
  }
  $input->{File} = delete $input->{$key};
  if ( exists $input->{PFNs} ) { $input->{File} = $input->{PFNs}; }
  $kernel->post( $reconstructor->Name, 'RecoIsPending', $input );

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
		Name		=> 'AOD::Receiver',
		Verbose		=> $verbose,
		Debug		=> $debug,
		Quiet		=> $quiet,
 		OnInput		=> \&OnInput,
	);

$reconstructor = T0::Component::Manager->new
	(
		Name		=> 'AOD::Manager',
		Config		=> $config,
                Verbose		=> $verbose,
                Debug		=> $debug,
                Quiet		=> $quiet,
		Logger		=> $sender,
		SelectTarget    => \&SelectTarget,
        );

$key   = $key   || $reconstructor->{InputKey} || 'RecoReady';
$skey  = $key . 'For' . $reconstructor->Name;
$value = $value || $reconstructor->{Value}    || '.*(raw|root)$';

SetProductMap;
Print $sender->Name,", running on ",$sender->Host,". My PID is ",$$,"\n";
Print $listener->Name,", running on ",$listener->Host,':',$listener->Port,"\n";

POE::Kernel->run();
exit;

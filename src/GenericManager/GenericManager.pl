#!/usr/bin/perl -w

use strict;
use POE;
use Getopt::Long;
use T0::Logger::Sender;
use T0::Logger::Receiver;
use T0::GenericManager::Manager;
use T0::Util;

my ($help,$verbose,$debug,$quiet,$config);
my ($manager,$sender,$listener,$generator);

sub usage
{
  die <<EOF;

  Usage $0 <options>

  ...no detailed help yet, sorry...

EOF
}

$help = $verbose = $debug = 0;
$config = "../Config/JulyPrototype.conf";
GetOptions(     "help"          => \$help,
                "verbose"       => \$verbose,
                "quiet"         => \$quiet,
                "debug"         => \$debug,
                "config=s"      => \$config,
          );
$help && usage;

sub subscribe
{
  my %h;
  $h{RPC}   = 'Subscribe';
  $h{Host}  = $listener->Host;
  $h{Port}  = $listener->Port;
  $h{Key}   = $manager->{Key};
  $h{Value} = $manager->{Value};

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
  return 0 if ( !exists($input->{$manager->{Key}}) || $input->{$manager->{Key}} !~ m%$manager->{Value}% );

  $self->Verbose("OnInput: input=$input, = ",join(' ',%$input),"\n");

  $input->{File} = delete $input->{$manager->{Key}};
  $kernel->post( $manager->Name, 'InputPending', $input );

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
		Name		=> 'Generic::Receiver',
		Verbose		=> $verbose,
		Debug		=> $debug,
		Quiet		=> $quiet,
 		OnInput		=> \&OnInput,
	);

$manager = T0::GenericManager::Manager->new
	(
		Config		=> $config,
                Verbose		=> $verbose,
                Debug		=> $debug,
                Quiet		=> $quiet,
		Logger		=> $sender,
        );

Print $sender->Name,", running on ",$sender->Host,". My PID is ",$$,"\n";
Print $listener->Name,", running on ",$listener->Host,':',$listener->Port,"\n";

POE::Kernel->run();
exit;

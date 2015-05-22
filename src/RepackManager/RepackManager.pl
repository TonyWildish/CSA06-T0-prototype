#!/usr/bin/perl -w

use strict;
use POE;
use Getopt::Long;
use T0::Logger::Sender;
use T0::Logger::Receiver;
use T0::RepackManager::Manager;
use T0::Util;

my ($help,$verbose,$debug,$quiet);
my ($config,$retry,$key,$value,$unsubscribe);
my ($repacker,$sender,$listener,$generator);

sub usage
{
  die <<EOF;

  Usage $0 <options>

  ...no detailed help yet, sorry...

EOF
}

$help = $verbose = $debug = 0;
$retry = 3;
$key   = 'InputReady';
$value = '.*(raw|idx)$';
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
                "value=s"      	=> \$value,
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
  $h{QueueEntries}  = $unsubscribe ? 0 : 1;

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

  $self->Verbose("OnInput: input=$input, =",join(' ',%$input),"\n");
  my (@a,$type,$lumiID,$smid,$smTot,$guid,$size);
  $_ = $input->{$key};
  s%^.*/%%;
  ($type,$lumiID,$smid,$smTot,$guid) = split('\.',$_);
  $self->Verbose("Type=$type, LumiID=$lumiID, SMInstance=$smid, SMTotal=$smTot, Guid=$guid\n");

  push @{$repacker->{lumi}{$lumiID}{Files}{$type}}, $input->{$key};
  if ( ! defined($repacker->{lumi}{$lumiID}{Start}) )
  {
    $repacker->{lumi}{$lumiID}{Start} = time;
    if ( defined($repacker->{SegmentTimeout}) )
    {
      $kernel->post( $repacker->{Name} => 'SetSegmentTimer' => $lumiID);
    }
  }
  $repacker->{lumi}{$lumiID}{NFiles}++;
  $repacker->{lumi}{$lumiID}{SMTot} = $smTot;
  $repacker->{lumi}{$lumiID}{sm}{$smid}{$type} = $guid;
  $repacker->{lumi}{$lumiID}{sm}{$smid}{size} = $input->{Size}
			if defined($input->{Size});
  $repacker->Verbose("$lumiID: ",$repacker->{lumi}{$lumiID}{NFiles},
		     " out of ", $smTot*2, " so far... ($smid/$type))\n");
  if ( $repacker->{lumi}{$lumiID}{NFiles} >= $smTot * 2 )
  {
    $kernel->post( $repacker->Name, 'SegmentIsComplete', $lumiID );
  }

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
		Name		=> 'Repack::Receiver',
		Verbose		=> $verbose,
		Debug		=> $debug,
		Quiet		=> $quiet,
 		OnInput		=> \&OnInput,
	);

$repacker = T0::RepackManager::Manager->new
	(
		Config		=> $config,
                Verbose		=> $verbose,
                Debug		=> $debug,
                Quiet		=> $quiet,
		Logger		=> $sender,
		SelectTarget    => \&SelectTarget,
        );

Print $sender->Name,", running on ",$sender->Host,". My PID is ",$$,"\n";
Print $listener->Name,", running on ",$listener->Host,':',$listener->Port,"\n";

POE::Kernel->run();
exit;

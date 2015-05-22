#!/usr/bin/perl -w

use strict;
use POE;
use Getopt::Long;
use T0::Logger::Sender;
use T0::Logger::Receiver;
use T0::Util;

my ($help,$verbose,$debug,$quiet);
my ($config,$retry,$key,$value);
my ($exporter,$sender,$listener,$generator);
my $debug_me=1;

sub usage
{
  die <<EOF;

  Usage $0 <options>

  ...no detailed help yet, sorry...

EOF
}

$help = $verbose = $debug = 0;
$retry = 3;
$key   = 'ExportReady';
$value = '.*';
$config = "../Config/JulyPrototype.conf";
GetOptions(     "help"          => \$help,
                "verbose"       => \$verbose,
                "quiet"         => \$quiet,
                "debug"         => \$debug,
                "retry"         => \$retry,
                "config=s"      => \$config,
          );
$help && usage;

sub subscribe
{
  my %h;
  $h{RPC}   = 'Subscribe';
  $h{Host}  = 'localhost';
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
$DB::single=$debug_me;

# Check this was really something I want
  return 0 if ( !exists($input->{$key}) || $input->{$key} !~ m%$value% );

  $self->Verbose("OnInput: input=$input, =",join(' ',%$input),"\n");
  my ($file,$cksum,$size,$dataset,$block);
  $file    = $input->{PFNs};
  $cksum   = $input->{CheckSums} || 0;
  $size    = $input->{Sizes} || 0;
  $dataset = $input->{Dataset};
  $block   = $input->{Block};
  chomp $dataset;
  chomp $block;

  $self->Print(" File $file (size=$size, cksum=$cksum)\n");
  if ( defined($self->{DropScript}) )
  {
    if ( ! defined($dataset) && defined($self->{T1Rates}) )
    {
      $dataset = bin_table($self->{T1Rates});
      $dataset = "T0-Test-" . (split('','ABCDEFGHIJKLMNOPQRSTUVWXYZ'))[$dataset];
    }
    my $c = $self->{DropScript} . " $file $size $cksum $dataset $block";
    open DROP, "$c |" or die "open: $c: $!\n";
    while ( <DROP> ) { $self->Verbose($_); }
    close DROP or die "close: $c: $!\n";
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
		Name		=> 'Export::Receiver',
		Verbose		=> $verbose,
		Debug		=> $debug,
		Quiet		=> $quiet,
 		OnInput		=> \&OnInput,
	);

Print $sender->Name,", running on ",$sender->Host,". My PID is ",$$,"\n";
Print $listener->Name,", running on ",$listener->Host,':',$listener->Port,"\n";

POE::Kernel->run();
exit;

#!/usr/bin/perl -w

use strict;
use POE;
use Getopt::Long;
use T0::Logger::Sender;
use T0::Logger::Receiver;
use T0::Util;

my ($help,$verbose,$debug,$quiet);
my ($config,$host,$port,$key,$newkey,$value,$idempotent,%seen);
my ($remotehost,$remoteport);
my ($sender,$listener,$name);

sub usage
{
  die <<EOF;

  Usage $0 <options>

  ...no detailed help yet, sorry...

EOF
}

$help   = $verbose = $debug = $idempotent = 0;
$host   = $remotehost = 'localhost';
$port   = 1027;
#$key    = 'RawReady';
#$newkey = 'DBSReady';
$value  = '.*';
$remoteport = 12346;
$config = "../Config/JulyPrototype.conf";
GetOptions(     "help"          => \$help,
                "verbose"       => \$verbose,
                "quiet"         => \$quiet,
                "debug"         => \$debug,
                "config=s"      => \$config,
                "key=s"      	=> \$key,
                "newkey=s"      => \$newkey,
                "name=s"      	=> \$name,
                "value=s"      	=> \$value,
		"idempotent=s"	=> \$idempotent,
          );
defined $name or die "\"--name\" obligatory\n";
$help && usage;

sub subscribe
{
  $key = $listener->{InputKey} unless defined($key);
  $newkey = $listener->{OutputKey} unless defined($newkey);

  my %h;
  $h{RPC}   = 'Subscribe';
  $h{Host}  = $listener->Host;
  $h{Port}  = $listener->Port;
  $h{Key}   = $key || $listener->{InputKey};
  $h{Value} = $value;
  $h{RetryInterval} = 3;
  $h{QueueEntries}  = 1;

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

# Check this was really something I want
  return 0 if ( !exists($input->{$key}) || $input->{$key} !~ m%$value% );

  $self->Quiet("Bridging $key -> $newkey for ",$input->{$key},"\n");
  $input->{$newkey} = delete $input->{$key};

# Hack to fix LFN thing...
  if ( exists($input->{RECOLFNs}) )
  {
    $_ = $input->{RECOLFNs};
    if ( ! m%^/% ) { $_ = '/' . $_; }
    if ( ! m%^/store% ) { $_ = '/store' . $_; }
    $input->{RECOLFNs} = $_;
  }

# Hack to fix DBSUpdate values...
  if ( $newkey eq 'DBSUpdate' && $input->{$newkey} == 1 )
  {
    $input->{$newkey} = 'DBS.RegisterReco';
  }

  if ( $idempotent && $input->{$idempotent} && $seen{$input->{$idempotent}}++ )
  {
    Print "Ignoring duplicate message for ",$input->{$idempotent},"\n"; 
    return 1;
  }
  $sender->Send( $input );

  return 1;
}

$sender = T0::Logger::Sender->new(
		Config		=> $config,
                Verbose		=> $verbose,
                Debug  		=> $debug,
                Quiet   	=> $quiet,
 		OnConnect	=> \&OnConnect,
 		OnError		=> sub { return 0; },
	);

$listener = T0::Logger::Receiver->new (
		Config		=> $config,
		Name		=> $name,
		Verbose		=> $verbose,
		Debug		=> $debug,
		Quiet		=> $quiet,
 		OnInput		=> \&OnInput,
	);

Print $sender->Name,", running on ",$sender->Host,". My PID is ",$$,"\n";
Print $listener->Name,", running on ",$listener->Host,':',$listener->Port,"\n";

POE::Kernel->run();
exit;

#!/usr/bin/perl -w

use strict;
use POE;
use Getopt::Long;
use T0::Logger::Sender;
use T0::Logger::Receiver;
use T0::Util;
use File::Temp qw/ tempfile /;

my ($help,$verbose,$debug,$quiet);
my ($config,$retry,$key,$value,$unsubscribe);
my ($sender,$listener,$generator);

#select STDERR; $| = 1;	# make unbuffered
#select STDOUT; $| = 1;	# make unbuffered

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
  $h{Host}  = $listener->{Host};
  $h{Port}  = $listener->{Port};
  $h{Key}   = $key;
  $h{Value} = $value;
  $h{RetryInterval} = $unsubscribe ? 0 : $listener->{RetryInterval};
  $h{QueueEntries}  = $unsubscribe ? 0 : $listener->{QueueEntries};

  Print "Subscribing to key=\"$key\" with value=\"$value\"\n";
  $sender->Send( \%h );
}

sub OnConnect
{
  subscribe;
  return 0;
}

sub OnInput
{
  my ( $self, $kernel, $heap, $input ) = @_[ OBJECT, KERNEL, HEAP, ARG0 ];

  # Check this was really something I want
  return 0 if ( !exists($input->{$key}) || $input->{$key} !~ m%$value% );

  $self->Verbose("OnInput: input=$input, = ",join(' ',%$input),"\n");

  delete $input->{$key};

  #
  # will have to create workers if processing here is too slow
  #

  my $run = $input->{RUNNUMBER};
  my $lumi = $input->{LUMISECTION};
  my $lfn = $input->{LFN};
  my $nevents = $input->{NEVENTS};
  my $filesize = $input->{FILESIZE};
  my $checksum = $input->{CHECKSUM};
  my $type = $input->{TYPE};
  my $appname = $input->{APP_NAME};
  my $appversion = $input->{APP_VERSION};

  #
  # figure out the primary and processed dataset
  #
  my $primds = undef;
  my $procds = 'Online';
  if ( $type eq 'streamer' or $type eq 'edm' )
    {
      if ( $input->{STREAM} eq '' )
	{
	  $primds = $input->{SETUPLABEL};
	}
      else
	{
	  $primds = $input->{SETUPLABEL} . '-' . $input->{STREAM};
	}
    }
  elsif ( $type eq 'lumi' )
    {
      $primds = 'CMS_LUMI';
    }
  elsif ( $type eq 'lumi-sa' )
    {
      $primds = 'CMS_LUMI';
    }
  elsif ( $type eq 'lumi-vdm' )
    {
      $primds = 'CMS_LUMI';
    }
  elsif ( $type eq 'pixdmp' )
    {
      $primds = 'PixelCalib';
      $procds = 'PixDmp';
    }
  else
    {
      print "DBS registration not supported for this input\n";
      return 1;
    }

  #
  # call python script to register file in DBS
  #
  my @args = ("/data/cmsprod/TransferTest/dbsupdate/registerOnlineFile.py");
  push(@args, "--run","$run");
  push(@args, "--lfn","$lfn");
  push(@args, "--size","$filesize");
  push(@args, "--cksum","$checksum");
  push(@args, "--primds","$primds");
  push(@args, "--procds","$procds");
  push(@args, "--type","$type");
  push(@args, "--appname","$appname");
  push(@args, "--appversion","$appversion");

  if ( defined $lumi )
    {
        push(@args, "--lumi","$lumi");
    }
  if ( defined $nevents )
    {
        push(@args, "--nevents","$nevents");
    }

  system(@args) == 0
    or print "DBS registration failed: $?\n";

  return 0;
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
		Name		=> 'DBSUpdate::Receiver',
		Verbose		=> $verbose,
		Debug		=> $debug,
		Quiet		=> $quiet,
 		OnInput		=> \&OnInput,
	);

#$dbsupdate = T0::DBSUpdate::Manager->new
#	(
#	       Name		=> 'DBSUpdate::Manager',
#	       Config		=> $config,
#	       Verbose		=> $verbose,
#	       Debug		=> $debug,
#	       Quiet		=> $quiet,
#	       Logger		=> $sender,
#	       SelectTarget    => \&SelectTarget,
#        );

#$key   = $key   || $manager->{InputKey} || 'ReadyForNewFileToCheck';
#$value = $value || $manager->{Value}    || '1';

$key   = $key   || 'DBSUpdate';
$value = $value || '1';

Print $sender->Name,", running on ",$sender->Host,". My PID is ",$$,"\n";
Print $listener->Name,", running on ",$listener->Host,':',$listener->Port,"\n";

POE::Kernel->run();
exit;

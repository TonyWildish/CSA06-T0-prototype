#!/usr/bin/perl -w

use strict;
use POE;
use Getopt::Long;
use T0::Logger::Sender;
use T0::Logger::Receiver;
use T0::Util;
use File::Basename;

my ($help,$verbose,$debug,$quiet,$config);
my ($manager,$sender,$listener,$generator);
my ($Key,$Value, $host, $port);
my %seen;


$Key   = '';
$Value = '.*';
my $debug_me=1;

my ($total_files,$total_safe,$total_failed,$total_wrongsize,$total_wrongcksum);
my ($total_copied);

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
 		"host=s"	=> \$host,
 		"port=s"	=> \$port,
          );
$help && usage;

sub subscribe
{
  my %h;
  $h{RPC}   = 'Subscribe';
  $h{Host}  = $listener->Host;
  $h{Port}  = $listener->Port;
  $h{Key}   = 'DAQFileClosed';
  $h{Value} = $Value;
  $h{RetryInterval} = $listener->{RetryInterval};
  $h{QueueEntries}  = $listener->{QueueEntries};

  $sender->Send( \%h );

  my %g = %h;
  $g{Key} = 'DAQFileStatusUpdate';
  $sender->Send( \%g );
}

sub OnConnect
{
  subscribe;
  return 0;
}

sub OnInput
{
  my ( $self, $kernel, $heap, $input ) = @_[ OBJECT, KERNEL, HEAP, ARG0 ];
  my ($cmd, @arr, $sh, $slevel);
  $slevel = undef;
  
   
# print "Got something here\n";
  
# Check this was really something I want
  return 0 unless ( exists($input->{DAQFileClosed}) || exists($input->{DAQFileStatusUpdate}) );

# Uncomment to dump incoming message
#  $self->Quiet("Got: ",
#               join(',',map { " $_ => " . $input->{$_} } sort keys %$input ),
#               "\n");

  $DB::single=1;

  return 1 if exists($input->{Resent});

  my $filename = $input->{FILENAME} if ( exists $input->{FILENAME} );

  if ( exists($input->{DAQFileClosed}) )
  {
#   Got a 'new file' record
    $total_files++;
    $self->Quiet("File $filename has been registered\n");
  }

  my %g = (
            MonaLisa => 1,
            Cluster  => $T0::System{Name},
            Node     => 'DAQWatcher',
      );
  if ( exists($input->{DAQFileStatusUpdate}) )
  {
    # DAQ.Checked DAQ.WrongCksum (DAQ.CheckFailed - not used yet)
    if ($input->{DAQFileStatusUpdate} eq "t0input.checked") {
      return 1 if ($seen{$filename} eq 'safe');
      $seen{$filename} = 'safe';


      $g{'Time to safe'} = time - $input->{T0FirstKnownTime};
      $g{'Total safe'} = ++$total_safe;
      $g{'Total unsafe'} = $total_files - $total_safe;
      $self->Quiet("File $filename has been flagged as safe\n");
    } elsif ($input->{DAQFileStatusUpdate} eq "t0input.copy_failed"){
      $g{'Total failed'} = ++$total_failed;
      $self->Quiet("File $filename has been flagged as failed\n");
    } elsif ($input->{DAQFileStatusUpdate} eq "t0input.copied"){    
      return 1 if ( ($seen{$filename} eq 'copied') || ($seen{$filename} eq 'safe') );
      $seen{$filename} = 'copied';

      $g{'Total copied'} = ++$total_copied;
      $self->Quiet("File $filename has been flagged as copied\n");
      $self->Quiet("Got: ",
               join(',',map { " $_ => " . $input->{$_} } sort keys %$input ),
               "\n");

    } elsif ($input->{DAQFileStatusUpdate} eq "CASTOR.WrongSize"){
      $g{'Wrong castor size'} = ++$total_wrongsize;
    } elsif ($input->{DAQFileStatusUpdate} eq "CASTOR.WrongCksum"){
      $g{'Wrong checksums'} = ++$total_wrongcksum;
    } else { $self->Quiet("*** UNKNOWN MSG RCVD:" . $input->{DAQFileStatusUpdate} . "***\n");};
  }

   $self->Quiet("Sending: ",
                join(',',map { " $_ => " . $g{$_} } sort keys %g ),
                "\n");
  $sender->Send( \%g );

  return 1;
}

$sender = T0::Logger::Sender->new
        (
                Name            => 'DAQ::Watcher::Sender',
                Config          => $config,
                Verbose         => $verbose,
                Debug           => $debug,
                Quiet           => $quiet,
                OnConnect       => \&OnConnect,
                OnError         => sub { return 0; },
        );

$listener = T0::Logger::Receiver->new
        (
                Config          => $config,
                Name            => 'DAQ::Watcher::Receiver',
                Verbose         => $verbose,
                Debug           => $debug,
                Quiet           => $quiet,
                OnInput         => \&OnInput,
        );


Print $sender->Name,", running on ",$sender->Host,". My PID is ",$$,"\n";
Print $listener->Name,", running on ",$listener->Host,':',$listener->Port,"\n";

POE::Kernel->run();
exit;

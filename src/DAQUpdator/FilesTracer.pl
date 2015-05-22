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

$Key   = '';
$Value = '.*';
my $debug_me=1;

my ($total_files,$total_safe,$total_failed,$total_wrongsize,$total_wrongcksum);
my ($total_copied);
my %files;

sub usage
{
  die <<EOF;

  Usage $0 <options>

  ...no detailed help yet, sorry...

EOF
}

$help = 0;
$verbose = $debug = 1;
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
  $h{Key}   = 'OnlineFile';
  $h{Value} = $Value;
  $h{RetryInterval} = $listener->{RetryInterval};
  $h{QueueEntries}  = $listener->{QueueEntries};

  $sender->Send( \%h );

  my %g = %h;
  $g{Key} = 'DAQFileStatusUpdate';
  $sender->Send( \%g );

  my %k = %h;
  $k{Key} = 'DAQFileClosed';
  $sender->Send( \%k );

  my %z=%h;
  $z{Key} = 'GetFilesInfo';
  $sender->Send( \%z );
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
  return 0 unless ( exists($input->{OnlineFile}) || exists($input->{DAQFileClosed}) || exists($input->{DAQFileStatusUpdate}) || exists($input->{GetFilesInfo}) );

#  $debug && print "Found something from stuff we need!\n";

# Uncomment to dump incoming message
#  $self->Quiet("Got: ",
#               join(',',map { " $_ => " . $input->{$_} } sort keys %$input ),
#               "\n") if (exists($input->{DAQFileClosed}));

  $DB::single=1;
  
  #return 1 if exists($input->{Resent});
  if ( exists($input->{OnlineFile}) )
  {
#   Got a 'new file' record
    $total_files++;
  }

  my %g = (
            MonaLisa => 1,
            Cluster  => $T0::System{Name},
            Node     => 'FilesTracer',
      );

  if ( exists($input->{DAQFileStatusUpdate}) )
  {
    ### If filename is not set.....
    $input->{FILENAME} = basename($input->{PFN}) unless (exists ($input->{FILENAME}));
    ### Forget about strangers
    return 0 unless (defined($files{$input->{FILENAME}})); 
 
    if ($input->{DAQFileStatusUpdate} eq "t0input.checked") {
      ### Forget about files passed through
      delete $files{$input->{FILENAME}};
      $debug && print "Deleted file $input->{FILENAME}";

#      $g{'Time to safe'} = time - $input->{T0FirstKnownTime};
#      $g{'Total safe'} = ++$total_safe;
#      $g{'Total unsafe'} = $total_files - $total_safe;
    } elsif ($input->{DAQFileStatusUpdate} eq "t0input.copy_failed"){
      #### Failed!!! =((
      $files{$input->{FILENAME}} = 'copy_failed';
      $debug && print "Copy failed registered for file $input->{FILENAME}\n";

#      $g{'Total failed'} = ++$total_failed;
    } elsif ($input->{DAQFileStatusUpdate} eq "t0input.copied"){    
      $files{$input->{FILENAME}} = 'copied';
      $debug && print "Copy registered for file $input->{FILENAME}\n";


#      $g{'Total copied'} = ++$total_copied;
    } elsif ($input->{DAQFileStatusUpdate} eq "CASTOR.WrongSize"){
      $files{$input->{FILENAME}} = 'check_failed';
      $debug && print "Check failed registered for file $input->{FILENAME}\n";


#      $g{'Wrong castor size'} = ++$total_wrongsize;
    } elsif ($input->{DAQFileStatusUpdate} eq "CASTOR.WrongCksum"){
      $files{$input->{FILENAME}} = 'check_failed';
      $debug && print "Check failed registered for file $input->{FILENAME}\n";



#№     $g{'Wrong checksums'} = ++$total_wrongcksum;
    } else { $self->Quiet("*** UNKNOWN MSG RCVD:" . $input->{DAQFileStatusUpdate} . "***\n");};
  
  } elsif ( exists($input->{DAQFileClosed}) ) {
    ### File has been first registered
    $files{$input->{FILENAME}} = 'registered' if (defined($input->{FILENAME}));
    $debug && print "File $input->{FILENAME} registered \n";
  }## Returning block. By now - printing to STDOUT
  elsif ( exists($input->{GetFilesInfo}) ) {
    $self->Quiet("Report on files:\n") ;
    map {print "$_ $files{$_}\n" if (($input->{GetFilesInfo} eq $files{$_}) || ($input->{GetFilesInfo} eq 'all'))} keys %files;
    $self->Quiet("Report on files finished\n") 
  };


#   $self->Quiet("Sending: ",
#                join(',',map { " $_ => " . $g{$_} } sort keys %g ),
#                "\n");
#  $sender->Send( \%g );

  return 1;
}

$sender = T0::Logger::Sender->new
        (
                Name            => 'DAQ::FilesTracer::Sender',
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
                Name            => 'DAQ::FilesTracer::Receiver',
                Verbose         => $verbose,
                Debug           => $debug,
                Quiet           => $quiet,
                OnInput         => \&OnInput,
        );


Print $sender->Name,", running on ",$sender->Host,". My PID is ",$$,"\n";
Print $listener->Name,", running on ",$listener->Host,':',$listener->Port,"\n";

POE::Kernel->run();
exit;

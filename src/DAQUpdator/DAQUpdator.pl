#!/usr/bin/perl -w

use strict;
use POE;
use Getopt::Long;
use T0::Logger::Sender;
use T0::Logger::Receiver;
use T0::Util;
#use File::Basename;
#use T0::DAQOps;
#use QueryFile;
#use Switch;
use DBI;

my ($help, $verbose, $debug, $quiet, $config);
my ($config, $key, $value, $unsubscribe);
my ($manager, $sender, $listener, $generator);

#select STDERR; $| = 1;	# make unbuffered
#select STDOUT; $| = 1;	# make unbuffered

sub usage
{
  die <<EOF;

  Usage $0 <options>

  ...no detailed help yet, sorry...

EOF
}



my $total_entries = 0;

my %daqinfo;



$help = 0;
$verbose = $debug = 1;
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
  return;
}

sub OnInput
{
  my ( $self, $kernel, $heap, $input ) = @_[ OBJECT, KERNEL, HEAP, ARG0 ];
  my ($sql, $sth, $filename, $safety, $rowid);

  # Check this was really something I want
  return 0 if ( !exists($input->{$key}) || $input->{$key} !~ m%$value% );

  $self->Verbose("OnInput: input=$input, = ",join(' ',%$input),"\n");

# Uncomment to dump incoming message
  $self->Quiet("Got: ",
               join(',',map { " $_ => " . $input->{$_} } sort keys %$input ),
               "\n");

  $self->Quiet("Got: ", $input->{DAQFileStatusUpdate}, "\n");

  if ( exists $input->{FILENAME} )
    {
      $filename = $input->{FILENAME};
    }
  else
    {
      print "ERROR: No FILENAME field in notification\n";
      return;
    }

  $DB::single=1;

  # connect to database
  my $dbi    = "DBI:Oracle:cms_rcms";
  my $reader = "CMS_STOMGR_W";
  my $dbh = DBI->connect($dbi,$reader,"qwerty");

  unless ( $dbh )
    {
      die "cannot connect to the DB: $DBI::errstr\n";
    };

  $sql = "select ROWID, safety from CMS_STOMGR.tier0_injection where filename = :filename";
  $debug && $self->Quiet("SQL Query is: select ROWID, safety from CMS_STOMGR.tier0_injection where filename = '$filename'\n");

  $sth = $dbh->prepare( $sql ) || die "failed prepare : $dbh->errstr\n";
  $sth->bind_param(':filename' => $filename ); 
  $sth->execute() || die "failed execute : $dbh->errstr\n";

  my $count = 0;
  while ( my ( $tmp1, $tmp2 ) = $sth->fetchrow_array() ) {
    $debug && $self->Quiet("Fetched: line = $count, ROWID = $tmp1, SAFETY = $tmp2\n");
    ( $rowid, $safety ) = ( $tmp1, $tmp2 ) if ( defined($tmp2) );
    ++$count;
  };

  $debug && $self->Quiet("ROWID: $rowid, safety: $safety\n");

  $sth->finish() || die "failed finish : $dbh->errstr\n";

  if ( $count == 0 )
    {
      $self->Quiet("ERROR: No file with FILENAME $filename found in database\n");
      return;
    }

  if ( $count > 1 )
    {
      $self->Quiet("ERROR: More then one file with FILENAME $filename found in database\n");
      return;
    }

  # Check input for what safety level to set
  my $newsafety = 0;
  if ( $input->{DAQFileStatusUpdate} eq "t0input.checked" )
    {
      $newsafety = 100;
    }
  elsif ( $input->{DAQFileStatusUpdate} eq "t0input.check_failed" )
    {
      $newsafety = 20;
    }
  elsif ( $input->{DAQFileStatusUpdate} eq "t0input.copied" )
    {
      $newsafety = 10;
    }
  elsif ( $input->{DAQFileStatusUpdate} eq "t0input.copy_failed" ) 
    { 
      $newsafety = 5; 
    } 
  else
    {
      $self->Quiet("ERROR: Do now know new safety level for $input->{DAQFileStatusUpdate}\n");
      return;
    }

  if ( $newsafety > $safety )
    {
      # change safety level

      $sql = "update CMS_STOMGR.tier0_injection set safety = :newsafety where ROWID = :ROW_ID";
      $debug && $self->Quiet("SQL Query is: update CMS_STOMGR.tier0_injection set safety = $newsafety where ROWID = '$rowid'\n");

      $sth = $dbh->prepare( $sql ) || die "failed prepare : $dbh->errstr\n";
      $sth->bind_param( ':newsafety' => $newsafety );
      $sth->bind_param( ':ROW_ID' => $rowid );
      $sth->execute() || die "failed execute : $dbh->errstr\n";
      $sth->finish() || die "failed finish : $dbh->errstr\n";

      $debug && $self->Quiet("$sth->rows updated\n");

      # if no rows updated - return
      if ( $sth->rows == 0 ) {
        $self->Quiet("No rows updated for file $filename and ROWID $rowid\n");
        return;
      };

      my $now = time;

      my %h = (
	       Component       => 'DAQUpdator',
	       Type            => 'Feedback',
	       Message         => "File $filename was updated to Safety level of $newsafety",
	       TSUpdated       => $now,
	       ProcessTiming   => $now - $input->{STOP_TIME},
	       T0ProcessTiming => $now - $input->{T0FirstKnownTime},	
	      );
      $sender->Send( \%h );

      my %g = (
	       MonaLisa         => 1,
	       Cluster          => $T0::System{Name},
	       Node             => 'DAQUpdator',
	       T0ProcessingTime => $now - $input->{T0FirstKnownTime},
	       total_entries    => $total_entries++,
	      );
      $sender->Send( \%g );
    }

  $dbh->disconnect() || die "failed disconnect : $dbh->errstr\n";

  return;
}

$sender = T0::Logger::Sender->new
        (
                Name            => 'DAQ::Updator::Sender',
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
                Name            => 'DAQ::Updator::Receiver',
                Verbose         => $verbose,
                Debug           => $debug,
                Quiet           => $quiet,
                OnInput         => \&OnInput,
        );

$key   = $key   || 'DAQFileStatusUpdate';
$value = $value || '.*';

Print $sender->Name,", running on ",$sender->Host,". My PID is ",$$,"\n";
Print $listener->Name,", running on ",$listener->Host,':',$listener->Port,"\n";

POE::Kernel->run();
exit;



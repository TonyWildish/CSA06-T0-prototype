#!/usr/bin/perl -w

use strict;
use POE;
use T0::Logger::Sender;
use T0::Index::Generator;
use T0::FileWatcher;
use T0::Iterator::Rfdir;
use T0::Util;
use Getopt::Long;

my ($help,$verbose,$debug,$quiet);
my ($config,$receiver,$sender,$generator,$iterator);
my ($file,@files,$size,@sizes,$dir,$interval);
my ($i,$svcclass);

sub usage
{
  die <<EOF;

  Usage $0 <options>

  ...no detailed help yet, sorry...

EOF
}

$i = 0;
$help = $verbose = $debug = 0;
$config = "/afs/cern.ch/user/w/wildish/public/T0/July/src/Config/AutoRepackTest.conf";
$dir    = "/castor/cern.ch/cms/T0Prototype/StorageManager";
$svcclass = 't0input';
GetOptions(     "help"          => \$help,
                "verbose"       => \$verbose,
                "quiet"         => \$quiet,
                "debug"         => \$debug,
                "config=s"      => \$config,
		"dir=s"		=> \$dir,
		"svcclass=s"	=> \$svcclass,
          );
$help && usage;

$ENV{STAGE_SVCCLASS} = $svcclass;

sub DoRepack
{
  my ( $kernel ) = $_[ KERNEL ];
  my ($index,$conf,$export,$i,$j,$k,$pos);

  ($file,$size) = $iterator->Next;
  exit 0 unless $file;

  $index  = 'index.txt';
  $conf   = 'conf.txt';
  $export = 'Export.dat';

# my %text;
# $text{Host} = $generator->{Host};
  $i = $j = $k = $pos = 0;
  open INDEX, "> $index" or die "open $index: $!\n";
  print INDEX "FileURL = rfio:///?svcClass=",$receiver->{SvcClass},"&path=$file\n";
  while ( $size )
  {
    $i = $receiver->{EventSize};
    if ( $i > $size ) { $i = $size; }
    $size -= $i;
    $j = $k++;
    if  ( $k >= scalar @{$receiver->{DatasetRateTable}} ) { $k = 0; }
    print INDEX "$j $pos $i\n";
    $pos += $i;
  }
  close INDEX;

  open CONF, ">$conf" or die "open: $conf: $!\n";
  print CONF <<EOF;
SelectStream = 0
OpenFileURL = $export
IndexFile = $index
CloseFileURL = $export
EOF

  my (%h,$t,$cmd);
  $cmd = $ENV{T0ROOT} . "/src/RepackManager/run_repack.sh";
  $h{EventSize} = $receiver->{EventSize};
  $h{DutyCycle} = scalar @{$receiver->{DatasetRateTable}};
  $h{Time} = time;
  open CMD, "$cmd $conf 2>&1 |" or die "$cmd: $!\n";
  my @lines;
  while ( <CMD> )
  {
    push @lines, $_;
    if ( m%wrote\s+(\d+)\s+% ) { $h{size} = $1; }
    foreach $t ( qw / open position read write / )
    {
      if ( m%rfio/$t=(\d+)/(\d+)/([0-9.]+)MB/([0-9.]+)ms% )
      {
        $h{'rfio_' . $t . '_calls'} = $1;
        $h{'rfio_' . $t . '_MB'   } = $3;
        $h{'rfio_' . $t . '_ms'   } = $4;
      }
    }
  }
  if ( ! close CMD )
  {
    print "Job died: output is...\n",@lines;
    die "close: $cmd: $!\n";
  }
  $h{Time} = time - $h{Time};
  $h{rate} = int(100*$h{size}/1024/1024/$h{Time})/100;
  print scalar localtime, ' : ';
  foreach ( sort keys %h ) { print "$_=$h{$_} "; }
  print "\n";
  $h{MonaLisa} = 1;
  $h{Cluster} = 'DutyCycle_' . delete $h{DutyCycle};
  $h{Farm}    = 'EventSize_' . delete $h{EventSize};
  $h{Name}    = $sender->{Name};
  $sender->Send(\%h);
  $kernel->delay_set( 'DoRepack', 1 );
  return 1;
}

sub OnConnect
{
  my ( $heap, $kernel ) = @_[ HEAP, KERNEL ];
  $receiver = $sender->{Receiver};

  $kernel->state( 'FileChanged' );
  my %param = ( File     => $config,
                Interval => 5,
		Client   => $sender->{Name},
		Event    => 'FileChanged',
              );

  $sender->{Watcher} = T0::FileWatcher->new( %param );

  $kernel->state( 'DoRepack', \&DoRepack );
  $kernel->yield( 'DoRepack' );
  return 0;
}

sub FileChanged
{
  my $file = $sender->{Config};
  return unless $file;

  $sender->Quiet("\"$file\" has changed...\n");

  T0::Util::ReadConfig($sender);

  if ( defined($sender->{Watcher}) )
  {
    $sender->{Watcher}->Interval($sender->{ConfigRefresh});
    $sender->{Watcher}->Options( %FileWatcher::Params);
  }
}

$sender = T0::Logger::Sender->new
	(
                Config 		=> $config,
		RetryInterval	=> 0,
                Verbose		=> $verbose,
                Debug  		=> $debug,
                Quiet   	=> $quiet,
 		OnError		=> sub { return 0; },
		OnConnect       => \&OnConnect,
	);

$iterator = T0::Iterator::Rfdir->new( Directory => $dir );
my $i = int(200 * rand);
print "Skipping $i entries...\n";
while ( $i-- ) { $iterator->Next; }

Print $sender->Name,  ", running on ",$sender->Host,". My PID is ",$$,"\n";

POE::Kernel->run();
exit;

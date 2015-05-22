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
my ($config,$sender,$generator,$iterator);
my ($file,@files,$size,@sizes,$dir,$interval);
my ($i);

sub usage
{
  die <<EOF;

  Usage $0 <options>

  ...no detailed help yet, sorry...

EOF
}

$i = 0;
$help = $verbose = $debug = 0;
$interval  = 1.0;
$config = "../Config/JulyPrototype.conf";
$dir    = "/castor/cern.ch/cms/T0Prototype/StorageManager";
GetOptions(     "help"          => \$help,
                "verbose"       => \$verbose,
                "quiet"         => \$quiet,
                "debug"         => \$debug,
                "config=s"      => \$config,
		"dir=s"		=> \$dir,
		"interval=f",	=> \$interval,
          );
$help && usage;

sub MakeFakeFile
{
  my ( $kernel ) = $_[ KERNEL ];
  ($file,$size) = $iterator->Next;
  exit 0 unless $file;

  my %t = ( InputReady => $file, Size => $size );
  $sender->Send( \%t );

  my %text;
  $text{InputReady} = $generator->Generate( file => $file,
                        		    size => $size,
                        		    protocol => '',
                      			  );
  $text{Host} = $generator->{Host},

  $sender->Send(\%text);

  my $rate;
  if ( defined($rate = $generator->{DataRate}) )
  {
    $interval = $size / (1024*1024) / $rate;
    $interval = int(1000*$interval)/1000;
    $generator->Verbose("Set interval=$interval for ",$rate," MB/sec\n");
  }

  if ( $interval ) { $kernel->delay_set( 'MakeFakeFile', $interval ); }
  else { $kernel->yield( 'MakeFakeFile' ); }
  return 1;
}

sub OnConnect
{
  my ( $self, $heap, $kernel ) = @_[ OBJECT, HEAP, KERNEL ];

  $kernel->state( 'FileChanged', $self );
  $self->{Watcher} = T0::FileWatcher->new( %param );

  $kernel->state( 'MakeFakeFile', \&MakeFakeFile );
  $kernel->yield( 'MakeFakeFile' );
  return 0;
}

sub FileChanged
{
  my $self = $_[ OBJECT ];
  my $file = $self->{Config};
  return unless $file;

  $self->Quiet("\"$file\" has changed...\n");

  T0::Util::ReadConfig($self);

  if ( defined($self->{Watcher}) )
  {
    $self->{Watcher}->Interval($self->{ConfigRefresh});
    $self->{Watcher}->Options( %FileWatcher::Params);
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

$generator = T0::Index::Generator->new
        (
                Config  => $config,
                Verbose => $verbose,
                Debug   => $debug,
                Quiet   => $quiet,
        );

$iterator = T0::Iterator::Rfdir->new( Directory => $dir );

Print $sender->Name,  ", running on ",$sender->Host,". My PID is ",$$,"\n";

POE::Kernel->run();
exit;

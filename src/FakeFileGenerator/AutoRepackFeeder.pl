#!/usr/bin/perl -w

use strict;
use POE;
use T0::Logger::Sender;
use T0::Index::Generator;
use T0::FileWatcher;
use T0::Util;
use Getopt::Long;
use Cwd;

my ($help,$verbose,$debug,$quiet);
my ($config,$link,$sender,$generator);
my ($file,$filesize,$smtot,$eventsize,$interval);
my ($i,$j,$lumi);
my $cwd = cwd;

sub usage
{
  die <<EOF;

  Usage $0 <options>

  ...no detailed help yet, sorry...

EOF
}

$i = $j = $lumi = 0;
$help = $verbose = $debug = 0;
$filesize  = 2 * 1024 * 1024 * 1024;
$eventsize = 1024 * 1024;
$smtot     = 10;
$interval  = 0.1;
$config = "../Config/JulyPrototype.conf";
GetOptions(     "help"          => \$help,
                "verbose"       => \$verbose,
                "quiet"         => \$quiet,
                "debug"         => \$debug,
                "config=s"      => \$config,
		"filesize=i"	=> \$filesize,
		"eventsize=i"	=> \$eventsize,
		"smtot=i",	=> \$smtot,
		"interval=f",	=> \$interval,
		"link=s"	=> \$link,
          );
$help && usage;

sub MakeFakeFile
{
  my ( $kernel ) = $_[ KERNEL ];
  if ( $i >= $smtot )
  {
    $i = 0;
    $lumi++;
  }
  $j++;
  $i++;
  $file = "$cwd/RAW.$lumi.$i.$smtot.$j.raw";
  if ( !defined $link )
  {
    open FILE, ">$file" or die "Cannot create empty $file: $!\n";
    close FILE;
  }
  else
  {
    link($link,$file) or
	symlink($link,$file) or
	die "Cannot (sym)link $file to $link\n";
  }

  my %t = ( InputReady => $file, Size => $filesize );
  $sender->Send( \%t );

  my %text;
  $text{InputReady} = $generator->Generate( file => $file,
                        		   size => $filesize,
                        		   protocol => '',
                      			 );
  $text{Host} = $generator->{Host},

  $sender->Send(\%text);

  if ( $interval ) { $kernel->delay_set( 'MakeFakeFile', $interval ); }
  else { $kernel->yield( 'MakeFakeFile' ); }
  return 1;
}

sub OnConnect
{
  my ( $self, $heap, $kernel ) = @_[ OBJECT, HEAP, KERNEL ];

  $kernel->state( 'MakeFakeFile', \&MakeFakeFile );
  $kernel->yield( 'MakeFakeFile' );
  return 0;
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

$watcher = T0::FileWatcher->new (
		File		=> $config,
		Object		=> $generator,
		Interval	=> $interval,
		Verbose		=> $verbose,
		Debug		=> $debug,
		Quiet		=> $quiet,
	);

$watcher->Interval($generator->ConfigRefresh);

$generator->{EventSizeMax} = $eventsize;
$generator->{EventSizeMin} = $eventsize;
$generator->{EventSizeStep} = 1;

Print $sender->Name, ", running on ",$sender->Host,". My PID is ",$$,"\n";

POE::Kernel->run();
exit;

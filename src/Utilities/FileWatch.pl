#!/usr/bin/perl -w

use strict;
use Carp;
use POE;
use Getopt::Long;
use T0::FileWatcher;
use T0::Util;

my ($help,$quiet,$verbose,$debug);
my ($file,$interval,$watcher);

sub usage
{
  die <<EOF;

  Usage $0 <options>

  ...no detailed help yet, sorry...

EOF
}

sub FileHasChanged
{
  my $file = shift;
  print "Apparently, \"$file\" has just changed...\n";
}

$help = $quiet = $verbose = $debug = 0;
$file = "../Config/DevPrototype.conf";
$interval = 1;
GetOptions(	"help"		=> \$help,
		"verbose"	=> \$verbose,
		"quiet"		=> \$quiet,
		"debug"		=> \$debug,
		"file=s"	=> \$file,
		"interval=i"	=> \$interval,
	  );
$help && usage;

$watcher = T0::FileWatcher->new (
		File		=> $file,
		Function	=> \&FileHasChanged,
		Interval	=> $interval,
		Verbose		=> $verbose,
		Debug		=> $debug,
		Quiet		=> $quiet,
	);

#Print "I am \"",$watcher->Name,"\", running on ",$watcher->Host,". My PID is ",$$,"\n";
POE::Kernel->run();
exit;

#!/usr/bin/env perl
use warnings;
use strict;
use Getopt::Long;
use T0::Index::Generator;

my ($help,$verbose,$debug,$quiet);
my ($config,$file,$size);

sub usage
{
  die <<EOF;

 Usage: $0 <options> {filelist}

 where <options> are:

 --help, --debug, --verbose, --quiet:	obvious...

 --config <string> : Name of configuration file driving this script, must
be locally readable.

 and {filelist} is a list of files, either local or accessible via RFIO

EOF
}

$verbose = $debug = $quiet = 0;
GetOptions(	"help"		=> \$help,
		"verbose"	=> \$verbose,
		"debug"		=> \$debug,
		"quiet"		=> \$quiet,
		"config=s"	=> \$config,
	  );

$help && usage;

-f $config or usage;
my $generator = T0::Index::Generator->new
	(
		Config	=> $config,
		Verbose	=> $verbose,
		Debug	=> $debug,
		Quiet	=> $quiet,
	);

foreach ( @ARGV )
{
  $generator->Generate( file => $_ );
#  $generator->Generate( file => $_,
#			size => 20*1024*1024*1024,
#			protocol => 'rfio',
#		      );
}

print "All done...\n";

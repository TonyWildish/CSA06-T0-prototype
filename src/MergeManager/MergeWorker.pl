#!/usr/bin/perl -w

use strict;
use POE;
use Getopt::Long;
use T0::Merge::Worker;
use T0::Logger::Sender;
use T0::Logger::Dashboard;
use T0::Util;

my ($help,$verbose,$debug,$quiet);
my ($config,$client,$logger,$dashboard);

select STDERR; $| = 1;	# make unbuffered
select STDOUT; $| = 1;	# make unbuffered

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
          );
$help && usage;

$logger = T0::Logger::Sender->new(
                Config  => $config,
                Verbose => $verbose,
                Debug   => $debug,
                Quiet   => $quiet,
      );

$dashboard  = T0::Logger::Dashboard->new (
                Config          => $config,
                Verbose         => $verbose,
                Debug           => $debug,
                Quiet           => $quiet,
                apmon           =>
                {
                        sys_monitoring  => 1,
                        general_info    => 1,
                },
        );

$client = T0::Merge::Worker->new(
		Name	  => 'Merge::Worker',
		Node	  => 'FastMerge',
		Config	  => $config,
                Verbose   => $verbose,
                Debug     => $debug,
                Quiet     => $quiet,
		Logger	  => $logger,
		Dashboard => $dashboard,
	);

$dashboard->Cluster($client->{Node});
$dashboard->Exe('EdmFastMerge');

Print "I am \"",$client->{Name},"\", running on ",$client->{Host},". My PID is ",$$,"\n";
POE::Kernel->run();
exit;

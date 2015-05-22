#!/usr/bin/perl -w

use strict;
use Carp;
use POE;
use POE::Component::Server::TCP;
use Getopt::Long;
use T0::Logger::Receiver;
use T0::Logger::Monalisa;
use T0::Util;

my ($help,$quiet,$verbose,$debug);
my ($config,$server,$apmon);

#sub POE::Kernel::ASSERT_DEFAULT () { 1 }

sub usage
{
  die <<EOF;

  Usage $0 <options>

  ...no detailed help yet, sorry...

EOF
}

$help = $quiet = $verbose = $debug = 0;
$config = "../Config/JulyPrototype.conf";
GetOptions(	"help"		=> \$help,
		"verbose"	=> \$verbose,
		"quiet"		=> \$quiet,
		"debug"		=> \$debug,
		"config=s"	=> \$config,
	  );
$help && usage;

$apmon  = T0::Logger::Monalisa->new (
		Config		=> $config,
		Verbose		=> $verbose,
		Debug		=> $debug,
		Quiet		=> $quiet,
		apmon		=>
		{
			sys_monitoring	=> 1,
			general_info	=> 1,
		},
	);
$server = T0::Logger::Receiver->new (
		Config		=> $config,
		Verbose		=> $verbose,
		Debug		=> $debug,
		Quiet		=> $quiet,
		ApMon		=> $apmon,
	);

Print "I am \"",$server->Name,"\", running on ",$server->Host,':',$server->Port,". My PID is ",$$,"\n";
POE::Kernel->run();
exit;

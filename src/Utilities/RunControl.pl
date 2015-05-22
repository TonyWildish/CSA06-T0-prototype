#!/usr/bin/perl -w

use strict;
use POE;
use T0::Logger::Sender;
use T0::Util;
use Getopt::Long;

my ($help,$verbose,$debug,$quiet,$config);
my ($sender,$state,$clients,$name,$host,$port);
$clients='.*';

sub usage
{
  die <<EOF;

  Usage $0 <options>

  ...no detailed help yet, sorry...

EOF
}

$help = $verbose = $debug = 0;
$config = "../Config/JulyPrototype.conf";
$name = 'Logger::Sender';
$host = 'localhost';
GetOptions(     "help"          => \$help,
                "verbose"       => \$verbose,
                "quiet"         => \$quiet,
                "debug"         => \$debug,
                "config=s"      => \$config,
                "name=s"        => \$name,
#                "host=s"        => \$host,
#                "port=i"        => \$port,
                "state=s"       => \$state,
                "clients=s"     => \$clients,
          );
defined($state) or die "\"--state\" required...\n";
#defined($port) or die "\"--port\" required...\n";
$help && usage;

POE::Session->create(
		     inline_states => {
				       _start => \&start_tasks,
				       quit   => sub { exit 0; },
				      },
		     args => [ ],
		    );

POE::Kernel->run();
exit;

sub start_tasks {
  my ( $kernel, $heap ) = @_[ KERNEL, HEAP ];

$DB::single=1;
  $sender = T0::Logger::Sender->new
    (
     Name	     => $name,
     Config 	     => $config,
     RetryInterval   => 0,
     Verbose	     => $verbose,
     Debug  	     => $debug,
     Quiet   	     => $quiet,
     OnError	     => sub { return 0; },
     OnConnect       => \&OnConnect,
    );

  Print $sender->Name, ", running on ", $sender->Host, ". My PID is ", $$, "\n";
  $kernel->delay_set( 'quit' => 2 );
}

sub OnConnect {
  my ( $kernel, $session ) = @_[ KERNEL, SESSION ];
  my %h = (
		SetState	=> $state,
		Clients		=> $clients,
	  );
  $sender->Name('Run::Control');
  $sender->Send( \%h );
}

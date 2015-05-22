#!/usr/bin/perl -w

use strict;
use POE;
use Getopt::Long;
use T0::Logger::Sender;
use T0::Util;

my ($help,$verbose,$debug,$quiet,$notimestamp);
my ($interval,$config,$retry);

sub usage
{
  die <<EOF;

  Usage $0 <options>

  ...no detailed help yet, sorry...

EOF
}

$notimestamp = $help = $verbose = $debug = 0;
$retry = 3;
$interval = 1.0;
$config = "../Config/JulyPrototype.conf";
GetOptions(     "help"          => \$help,
                "verbose"       => \$verbose,
                "quiet"         => \$quiet,
                "debug"         => \$debug,
                "retry"         => \$retry,
                "config=s"      => \$config,
		"notimestamp"	=> \$notimestamp,
                "interval=i"    => \$interval,
          );
$help && usage;

sub OnConnect
{
  my ( $self, $heap, $kernel ) = @_[ OBJECT, HEAP, KERNEL ];

# Only start the timer first time round, or there will be one timer per time
# the receiver is restarted
  return 0 if $heap->{count};
  print "OnConnect: self=$self, heap=$heap\n";
  $kernel->state( 'timer', \&timer );
  $kernel->yield( 'timer' );
  return 0;
}

my $sender = T0::Logger::Sender->new(
		Config		=> $config,
                Verbose		=> $verbose,
                Debug  		=> $debug,
                Quiet   	=> $quiet,
		RetryInterval	=> $retry,
		OnConnect	=> \&OnConnect,
	);

Print "I am \"",$sender->Name,"\", running on ",$sender->Host,". My PID is ",$$,"\n";

sub Log { $sender->Send(@_); }

sub timer
{
  my ( $heap, $kernel ) = @_[ HEAP, KERNEL ];
  my %h;
  $_ = <STDIN>;
  chomp;
  s%^[^{]*%%;
  #Stop sending
  if ( m%^\s*$% )
  {
    print "Dying, for lack of input...\n";
    $kernel->yield( 'forceShutdown' );
  }
  #Continue sending
  else
    {
      eval "%h = %{$_}";
      if ( ! $notimestamp )
	{
	  my $t = time;
	  $h{ResendEpochTime} = $t;
	  $h{ResendTime} = scalar localtime $t;
	}
      print "Sending: ",T0::Util::strhash(\%h),"\n";
      Log( \%h );
      if ( $interval > 0 )
	{
	  $kernel->delay_set( 'timer', $interval );
	}
      else
	{
	  $kernel->yield( 'timer' );
	}
    }
}

POE::Kernel->run();
exit;

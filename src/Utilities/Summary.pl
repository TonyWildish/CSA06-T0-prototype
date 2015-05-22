#!/usr/bin/perl -w
use strict;

use Getopt::Long;
my ($previous,$twophase,$num,$log,$totalEvents,@totals,$date,$tot,$rate);
my $base = 32660085
         +  3151891
         + 18504702
         + 49122382
         + 17211456
         + 11201843;
$log = "/data/CSA06/logs/Logger.log";
$twophase = $previous = 0;
$num = 4;
#$twophase = 1;
GetOptions(	"logfile=s"	=> \$log,
		"num=i"		=> \$num,
		"twophase"	=> \$twophase,
	  );

open LOG, "grep TotalEvents $log | grep PromptReco|" or die "open $log: $!\n";
while ( <LOG> )
{
  next unless m%^(.*): {.*'TotalEvents' => '(\d+)'%;
  $date = $1;
  $tot  = $2 + $base;
  if ( $twophase )
  {
    if ( $previous )
    {
      $tot += $previous - $base;
      push @totals, "$date : $tot";
      $previous = 0;
    }
    else
    {
      $previous = $tot;
    }
  }
  else
  {
    push @totals, "$date : $tot";
  }
}

my @a;
my $i = -1;
while ( exists($totals[$i]) )
{
  unshift @a, $totals[$i];
  $i -= 12;
}
@totals=@a;

print "Recent total event readings:\n";
$i = $#totals;
if ( $num > 0 )
{
  if ( $i > $num ) { $i = $num; }
}
else { $i = $i > 4 ? 4 : $i; }
print join("\n", @totals[ -1-$i .. -1]);
$rate = ((split(' ',$totals[-1]))[-1] - (split(' ',$totals[-1-$i]))[-1])/$i/3600;
$rate = int($rate*100)/100;
print "\n Rate over the last $i hours: $rate Hz\n";

#!/usr/bin/env perl
use warnings;
use strict;
use Getopt::Long;

my ($help,$verbose,$debug,$min,$max,$step);
my ($kb,$mb,$gb,$c,%u,$unit,$file);
my ($i,$j,$dd,$etc,$start,$log,@sizes);

$kb = 1024;
$mb = $kb * $kb;
$gb = $kb * $kb * $kb;
%u = ( 'k' => $kb, 'K' => $kb,
       'm' => $mb, 'M' => $mb,
       'g' => $gb, 'G' => $gb
     );

sub usage
{
  die <<EOF;

 Usage: $0 <options>

 where <options> are:

 --help, --debug, --verbose:	obvious...

 --min, --max, --step:	string specification of minimum and maximum filesizes,
 and the step between them. The string is an integer followed by an optional
 'K|k', 'M|m', or 'G|g', for KB, MB, or GB respectively. These arguments are
 all mandatory.

 --log: Generate a logarithmically-sequenced set of files, not linear.

  Files are created reading from /dev/random. The first filesize is \$min
 bytes, and files are continually created \$step bytes larger than the
 previous one until \$max is reached (or passed, if the alignment isn't
 perfect).

  Filenames are derived from the smallest unit describing the input parameters,
 and are 'File-\$size\$unit.dat'.

EOF
}

$verbose = $debug = 0;
GetOptions(	"min=s"		=> \$min,
		"max=s"		=> \$max,
		"step=s"	=> \$step,
		"log"		=> \$log,
		"help"		=> \$help,
		"verbose"	=> \$verbose,
		"debug"		=> \$debug
	  );

$help && usage;
defined($min) && defined($max) && defined($step) or usage;

$min =~ m%(\d+)([K,k,M,m,G,g])%  or usage;
$min = $1;
$unit = $u{$2};

$max =~ m%(\d+)([K,k,M,m,G,g])%  or usage;
$max = $1;
$c = $u{$2};
if ( $c > $unit ) { $max = $max * $c / $unit; }
if ( $c < $unit ) { $min = $min * $unit / $c; $unit = $c; }

$step =~ m%(\d+)([K,k,M,m,G,g])%  or usage;
$step = $1;
$c = $u{$2};
if ( $c > $unit )
{
  $step = $step * $c / $unit;
}
if ( $c < $unit )
{
  $min = $min * $unit / $c;
  $max = $max * $unit / $c;
  $unit = $c;
}

foreach ( 'K', 'M', 'G' )
{
  if ( $unit =~ m%$u{$_}% ) { $unit = $_; }
}
print "Min=$min, Max=$max Step=$step Unit=${unit}B\n";
$i = $min;
push @sizes, $i;
$j = 0;
do
{
  $j += $i;
  $i += $step;
  push @sizes, $i;
  if ( $log ) { $step *= 2; }
} while ( $i <= $max );
print "Total size on disk: $j ${unit}B\n";

$start = time;
$etc = "(unknown)";

$i = 0;
foreach $min ( @sizes )
{
  $i += $min;

  $file = sprintf("File-%08i${unit}B.dat",$min);
  print scalar localtime, ": Creating $file\n";
  if ( -f $file )
  {
    print "$file exists, won't clobber!\n";
    next;
  }
  if ( $unit eq 'G' )
  { $dd = "dd if=/dev/urandom of=$file bs=1m count=" . $min * $kb; }
  else
  { $dd = "dd if=/dev/urandom of=$file bs=1$unit count=$min"; }
  open DD, "$dd 2>&1 |" or die "open: $dd: !$\n";
  while ( <DD> ) { $verbose && print; }
  close DD or die "close: $dd: !$\n";

  $etc = int( (time - $start)*($j/$i) + $start);
  print "Estimate time of completion: ", scalar localtime $etc, "\n";
}

print "All done...\n";

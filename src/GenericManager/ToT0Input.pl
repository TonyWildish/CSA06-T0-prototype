#!/usr/bin/perl -w
use strict;
use File::Basename;
use Getopt::Long;

my ($src,$dst,$dir,$file,$ofile,$verbose,$svcclass);
my ($ddir,$doit);

GetOptions(
		'File=s'	=> \$file,
	  );

$src = '/castor/cern.ch/cms/store/CSA06';
$dst = '/castor/cern.ch/cms/T0/Input/CSA06';
$svcclass = 't0input';
$verbose  = 1;

sleep 1;
exit 0 unless $file =~ m%^$src%;
($ofile = $file) =~ s%$src%$dst%;

$doit = 0;
open RFSTAT, "rfstat $ofile 2>&1 |" or die "rfstat: $ofile: $!\n";
while ( <RFSTAT> )
{
  if ( m%No such file or directory% )
  {
    $doit = 1;
    last;
  }
}
close RFSTAT;
if ( ! $doit )
{
  $verbose && print "Exiting, seen this one already...\n";
  exit 0;
}

print "Copy $file\n";
open RFCP, "rfcp $file . |" or die "rfcp $file .: $!\n";
while ( <RFCP> ) { print; }
close RFCP or die "close rfcp $file $ofile: $!\n";

print "Write $ofile\n";
$file = basename $file;
open RFCP, "rfcp $file 'rfio:///?svcClass=$svcclass\&path=$ofile' |" or die "rfcp $file $ofile: $!\n";
while ( <RFCP> ) { print; }

close RFCP or do
{
  $ddir = dirname $ofile;
  open RFMKDIR, "rfmkdir -p $ddir |" or die "rfmkdir $ddir: $!\n";
  while ( <RFMKDIR> ) { $verbose && print; }
  close RFMKDIR;

  open RFCP, "rfcp $file 'rfio:///?svcClass=$svcclass\&path=$ofile' |" or die "rfcp $file $ofile: $!\n";
  while ( <RFCP> ) { print; }
  close RFCP or die "close rfcp $file $ofile: $!\n";
};

unlink $file;
print "Finished...\n";

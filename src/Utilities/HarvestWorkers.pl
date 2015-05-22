#!/usr/bin/perl -w
use strict;
use File::Basename;

my (@a,%jobs,%g,%h,$log,$job,$host,$dir,$killit);
open JOBS, "<joblist" or die "Cannot find joblist: $!\n";
while ( <JOBS> )
{
  chomp;
  m%^(\d+)\s+(\S+)%;
  $jobs{$1} = $2;
}
close JOBS;

open LOG, "egrep 'RecoReady|AlcarecoReady' /data/CSA06/logs/Logger.log |" or die "open: $!\n";
while ( <LOG> )
{
  m%WNLocation% or next;
  s%^[^{]*%%;
  eval "%h = %{$_}";
  @a = split('/',$h{WNLocation});
# print "Checking $h{WNLocation}\n";
  next unless exists $jobs{$a[4]};
  $job  = $a[4];
# print "Take $job\n";
  $host = $jobs{$job};
  $dir  = $a[5];
}

#print "Processing: \n\t",join("\n\t", map {"$_ => $h{$_}" } sort keys %h),"\n";

die "Nothing found!\n" unless $job;
print "Job $job, working dir $dir\n";
#open STOP, "bstop $job |" or die "Cannot stop job $job: $!\n";
#while ( <STOP> ) { print; }
#close STOP or die "Error stopping $job: $!\n";

open RFDIR, "rfdir $host:/pool/lsf/cmsprod/$job |" or die "rfdir: $!\n";
while ( <RFDIR> )
{
  chomp;
  s%^.* %%;
  m%^w_2006% or next;
  $g{$_}++;
}
close RFDIR or die "Close rfdir: $!\n";

$killit = 1;
foreach ( sort keys %g )
{
  if ( $_ gt $dir ) { print "$_ is greater than $dir\n"; $killit=0; }
  if ( $_ eq $dir ) { print "$_ is equal to     $dir\n"; }
  if ( $_ lt $dir ) { print "$_ is less than    $dir\n"; }
}

if ( $killit )
{
  print "Kill $job\n";
  open KILL, "bkill $job |" or die "bbkill $job: $!\n";
  while ( <KILL> ) { print; }
  close KILL or die "close bkill: $!\n";
}
else
{
  print "Resume $job\n";
#  open RESUME, "bresume $job |" or die "bresume $job: $!\n";
#  while ( <RESUME> ) { print; }
#  close RESUME or die "close bresume: $!\n";
}

print "All done!\n";

#!/usr/bin/perl -w
use strict;
use warnings;
use POE;
use T0::Iterator::Rfdir;
use T0::Copy::Rfcp;
use T0::Merge::FastMerge;
use T0::Util;
use File::Basename;
use Getopt::Long;

my $help;

my %fileStatusList;

select STDERR; $| = 1;	# make unbuffered
select STDOUT; $| = 1;	# make unbuffered

sub usage
{
  die <<EOF;

  Usage $0 <options>

  ...no detailed help yet, sorry...

EOF
}

GetOptions(
	   "help"       => \$help,
          );
$help && usage;

POE::Session->create(
		     inline_states => {
				       _start => \&start_task,
				       start_rfdir => \&start_rfdir,
				       rfdir_done => \&rfdir_done,
				       start_rfcp => \&start_rfcp,
				       rfcp_done => \&rfcp_done,
				       start_merge => \&start_merge,
				       merge_done => \&merge_done,
				      },
		    );

POE::Kernel->run();
exit;

sub start_task {
  my ( $kernel, $heap, $session ) = @_[ KERNEL, HEAP, SESSION ];

  my %inputhash = (
		   Session => $session,
		   Callback => 'rfdir_done',
		   Directory => "/castor/cern.ch/cms/T0Prototype/Alca/092/minbias",
#		   Directory => "cmslcgse02:/data1/hufnagel/T0/PRInput",
#		   Directory => "/castor/cern.ch/cms/T0Prototype/hufnagel/MergeInput",
		   MinAge => "5",
		   Files => {
			     Status => \%fileStatusList,
			    }
		  );

  print localtime() . " : Start rfdir in " . $inputhash{Directory} . "\n";

  T0::Iterator::Rfdir->new(\%inputhash);
}

sub rfdir_done {
  my ( $kernel, $heap, $session ) = @_[ KERNEL, HEAP, SESSION ];

  print localtime() . " : Finished rfdir\n";

  $kernel->yield('start_merge');
}

sub start_merge {
  my ( $kernel, $heap, $session ) = @_[ KERNEL, HEAP, SESSION ];

  my $merge_number = 50;
  my $merge_from_local = 1;

  my $count = 0;

  my %rfcphash = (
		  session => $session,
		  callback => 'rfcp_done',
		  timeout => 600, # timeout for rfcp call
		  files => []
		 );

  my %mergehash = (
		   session => $session,
		   callback => 'merge_done',
		   timeout => 3600, # timeout for FastMerge
		   sources => [],
		   target => 'file:///data/T0Prototype/RecoTesting/MergeOutput/merged.root',
		  );


  # loop over files, take the first $count for copying/merging
  foreach my $filename ( sort keys %fileStatusList )
    {
#      next if ( basename($filename) eq '0049E6C0-7C2E-DB11-B416-00096B8C7642.AlcaPhySym.root' );

      $count++;

      push(@{ $rfcphash{files} }, { source => $filename, target => '/data/T0Prototype/RecoTesting/MergeInput' } );

      if ( $merge_from_local == 1 )
	{
#	  print "Add source file:///data/T0Prototype/RecoTesting/MergeInput/" . basename($filename) . "\n";
	  push(@{ $mergehash{sources} }, 'file:///data/T0Prototype/RecoTesting/MergeInput/' . basename($filename) );
	}
      else
	{
#	  print "Add source rfio:" . $filename . "\n";
	  push(@{ $mergehash{sources} }, 'rfio:' . $filename );
	}

      last if ( $count >= $merge_number );
    }

  $ENV{STAGE_SVCCLASS} = 't0export';

  if ( $merge_from_local == 1 )
    {
      $heap->{MergeHash} = \%mergehash;

      print localtime() . " : Copy started\n";
      T0::Copy::Rfcp->new(\%rfcphash);
    }
  else
    {
      print localtime() . " : Merge started\n";
      T0::Merge::FastMerge->new(\%mergehash);
    }

  return 1;
}

sub rfcp_done
{
  my ( $kernel, $heap, $hash_ref ) = @_[ KERNEL, HEAP, ARG0 ];

  print localtime() . " : Copy Done\n";

  my $count = 0;
  foreach my $file ( @{ $hash_ref->{files} } )
    {
      $count++;
      if ( $file->{status} != 0 )
	{
	  print localtime() . " : rfcp " . $file->{source} . " " . $file->{target} . " returned " . $file->{status} . "\n";
	}
    }

  print localtime() . " : Copied " . $count . " files\n";

  print localtime() . " : Merge started\n";
  T0::Merge::FastMerge->new($heap->{MergeHash});
}

sub merge_done
{
  my ( $kernel, $heap, $hash_ref ) = @_[ KERNEL, HEAP, ARG0 ];

  print localtime() . " : Merge Done\n";
}

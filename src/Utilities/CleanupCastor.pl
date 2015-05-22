#!/usr/bin/perl -w
use strict;
use POE;
use T0::Iterator::Rfdir;
use Getopt::Long;

my %fileStatusList;
my $maxFileNumber = 5;
my $rootDirectory = '/castor/cern.ch/cms/T0';
my ($hammer,$verbose,$directory);

$hammer = $verbose = 0;
GetOptions(     "verbose"     => \$verbose,
		"hammer"      => \$hammer,
                "directory=s" => \$directory,
          );

defined($directory) or die "Expecting a '--directory'\n";
$directory =~ m%^$rootDirectory% or die "Won't kill files outside $rootDirectory!\n";

POE::Session->create(
		     inline_states => {
				       _start => \&start_task,
				       rfdir_done => \&rfdir_done,
				      },
		     args => [ ],
		    );

POE::Kernel->run();
exit;

sub start_task {
  my ( $kernel, $heap, $session ) = @_[ KERNEL, HEAP, SESSION ];

  my %inputhash = (
		   Session => $session,
		   Callback => 'rfdir_done',
		   Directory => $directory,
		   Files => {
			     Status => \%fileStatusList,
			    },
		  );

  T0::Iterator::Rfdir->new(\%inputhash);
}

sub rfdir_done {
  my ( $kernel, $heap ) = @_[ KERNEL, HEAP ];

  my ($cmd,@files);
  my $fileCount = 0;

  foreach my $file ( keys %fileStatusList )
    {
      $verbose && print "$file\n";
      push @files, $file;
      if ( scalar @files == $maxFileNumber )
	{
	  $fileCount += $maxFileNumber;

	  $cmd = "stager_rm -M " . join(' -M ',@files);
	  $verbose && print $cmd,"\n";
	  open CMD, "$cmd |" or die "$cmd: $!\n";
	  while ( <CMD> ) { print if $verbose; }
	  close CMD or die "close: $cmd: $!\n";
	  @files=();

	  print "Cleaned $fileCount files...      \n";
	}
    }

  if ( scalar @files > 0 )
    {
      $fileCount += scalar @files;

      $cmd = "stager_rm -M " . join(' -M ',@files);
      $verbose && print $cmd,"\n";
      open CMD, "$cmd |" or die "$cmd: $!\n";
      while ( <CMD> ) { print if $verbose; }
      close CMD or die "close: $cmd: $!\n";
      @files=();

      print "Cleaned $fileCount files...      \r";
    }

  if ( $hammer and $fileCount > 0 )
    {
      print "\nFinal pass, hammering them to death in 30 seconds...\n";
      sleep 30;

      $fileCount = 0;

      foreach my $file ( keys %fileStatusList )
	{
	  $fileCount++;

	  $cmd = "stager_rm -M $file";
	  $verbose && print $cmd,"\n";
	  open CMD, "$cmd |" or die "$cmd: $!\n";
	  while ( <CMD> ) { print if $verbose; }
	  close CMD or warn "close: $cmd: $!\n";

	  $cmd = "nsrm $file";
	  $verbose && print $cmd,"\n";
	  open CMD, "$cmd |" or die "$cmd: $!\n";
	  while ( <CMD> ) { print if $verbose; }
	  close CMD or warn "close: $cmd: $!\n";
	}

      print "Finished. Hammered $fileCount files...\n";
    }
}

1;

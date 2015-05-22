#!/usr/bin/perl -w
use Getopt::Long;
my ($file,$tape);
GetOptions( 'tape=s' => \$tape,
	    'file=s' => \$file
	  );
print "Pretend to issue a prestage command for $file from $tape\n";
sleep 1;
exit 0;

#!/usr/bin/perl -w

use strict;
use Getopt::Long;
use DBI;
use File::Basename;

my ($help,$config, $verbose, $debug);
my ($runnumber,$lumisection,$instance,$stoptime,$filename,$pathname);
my ($hostname,$dataset,$stream,$status,$type,$safety,$nevents,$filesize);
my ($starttime,$checksum);
my $sender;
my ($delay, $ofname, $time_t, $uptorun);
my @injl;

#########
my $exec = "perl /nfshome0/cmsprod/TransferTest/injection/sendNotification.pl";
########


sub usage
{
  $0 = basename($0);

  die <<EOF;

  Usage $0 --config=<config_file> [--help] [--delay=<delay_between_injections>] [--dataset=<dataset_name>] 
[--filename=<filename>] [time_t=<time_threshold>] [--ofname=<output_script_path>] [--verbose] [--uptorun=<runnumber>]
[--safety=<less_than_what>]

  Almost all the parameters are obvious. Non-obvious ones are:

	--delay           sets sleeping time before next injection is made (seconds).
			  Default value is 3.

	--time_t	  sets minimum time period between file is registered in DAQ 
			  DB and execution moment for script to inject only those 
			  files that older than value calculated as <localtime>-<delay>.
			  Default value is 60*60*3. 

			  ATTENTION!!! If "time_t" is not specified timing threshold will 
			  be default (NOT 0)
	
	--ofname	  unless set injection is made in runtime. Sets output filename
			  if immediate execution is not needed but injection bash script 
			  generation only.

	--uptorun 	  sets a run number (exlusively) up to which files are being 
			  injected.
			  
EOF
}

$help = $debug = 0;
$delay = 3;
$filename = '';
$time_t = 60*60*3;
$uptorun = 0;
GetOptions(
           "help"          => \$help,
           "config=s"      => \$config,
	   "delay=s"	   => \$delay,
	   "filename=s"	   => \$filename,
	   "dataset=s"	   => \$dataset,
	   "ofname=s"	   => \$ofname,
	   "time_t=s"	   => \$time_t,
	   "verbose"	   => \$verbose,
	   "safety=s"	   => \$safety,
	   "uptorun=s"	   => \$uptorun
	  );

$help && usage;

die "Configuration file is not set correctly. Use $0 --help for help\n" if (!defined($config));

###
### Script takes all the files with specified safety DAQ level and timestamp and sends them
###

### Query DB subroutine. Fills hash of files to be sent
sub GetFiles {

  my ($sql, $sth);
  my ($where_block, $localtime, $row);

  $localtime = time;

  # Creating WHERE block
  $where_block = " STATUS = 'closed' ";
  $where_block .= " AND STOP_TIME < ($localtime - $time_t)";
  $where_block .= " AND FILENAME = '$filename'" if ($filename ne '');  
  if ($safety){ 
    $where_block .= " AND SAFETY < '$safety'";
   } else { 
    $where_block .= " AND SAFETY < 100";
  }
  $where_block .= " AND DATASET = '$dataset'" if ($dataset);
  $where_block .= " AND RUNNUMBER < $uptorun" if ($uptorun > 0);
#  $where_block .= "AND " if ();
#  $where_block .= "AND " if ();



  # DB things
  $DB::single=1;

  # connect to database
  my $dbi = "DBI:Oracle:omds";
  my $reader = "cms_sto_mgr";
  my $dbh = DBI->connect($dbi,$reader,"qwerty");

  unless ( $dbh )
    {
      die "cannot connect to the DB: $DBI::errstr\n";
    };

  $sql = "select RUNNUMBER,
		 LUMISECTION,
		 INSTANCE,
		 COUNT,
		 START_TIME,
		 STOP_TIME,
		 FILENAME,
		 PATHNAME,
		 HOSTNAME,
		 DATASET,
		 STREAM,
		 STATUS,
		 TYPE,
		 SAFETY,
		 NEVENTS,
		 FILESIZE,
		 CHECKSUM
	    from cms_sto_mgr_admin.tier0_injection 
	   where $where_block
	   order by STOP_TIME";

  $verbose && print "SQL statement to be used is: $sql\n";

  $sth = $dbh->prepare( $sql ) || die "failed prepare : $dbh->errstr\n";
  $sth->execute() || die "failed execute : $dbh->errstr\n";

  my $count = 0;
  while ( my @fd = $sth->fetchrow_array() ) {
    
    $row = "$exec --config=$config --RUNNUMBER=$fd[0] --LUMISECTION=$fd[1] --INSTANCE=$fd[2] --COUNT=$fd[3] --START_TIME=$fd[4] --STOP_TIME=$fd[5] --FILENAME=$fd[6] --PATHNAME=$fd[7] --HOSTNAME=$fd[8] --DATASET=$fd[9] --STREAM=$fd[10] --STATUS=$fd[11] --TYPE=$fd[12] --SAFETY=$fd[13] --NEVENTS=$fd[14] --FILESIZE=$fd[15] --CHECKSUM=$fd[16]";

    $debug && print("$row\n");

    push @injl, $row;

    ++$count;
  };

  $verbose && print "Rows fetched: $count\n";

  $sth->finish() || die "failed finish : $dbh->errstr\n";


};

### Getting command lines
&GetFiles();

### Output
my ($count, $sstr, $estr, $sleepstr, $env);
$sstr = "Executing statement";
$estr = "of " . @injl;
$sleepstr = "sleep $delay";
$count = 0;
$env = 'export T0_BASE_DIR=/nfshome0/cmsprod/TransferTest
export T0ROOT=${T0_BASE_DIR}/T0
export CONFIG=${T0_BASE_DIR}/Config/TransferSystem_Cessy.cfg
export PERL5LIB=${T0ROOT}/perl_lib:${T0_BASE_DIR}/perl';


if ($ofname) {
  
  open (FILE, ">$ofname") or die "$!\n";

  print FILE "echo 'Setting environment' \n$env\n\n";

  map {print FILE "echo '$sstr ". (++$count) . " $estr' \n$_\n$sleepstr\n"} @injl;

  close(FILE);

} else {
  for $_ (@injl) {
    print "$sstr ". (++$count) . " $estr";
    
    $verbose && print "To be executed: $_\n";
    `$env; $_`;
    sleep $delay;
  };
};

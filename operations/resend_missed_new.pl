#!/usr/bin/perl -w

use strict;
use Getopt::Long;
use DBI;
use File::Basename;

my ($help,$config, $verbose, $debug);
my ($runnumber,$lumisection,$instance,$stoptime,$filename,$pathname);
my ($hostname,$setuplabel,$stream,$status,$type,$safety,$nevents,$filesize);
my ($starttime,$checksum);
my $sender;
my ($delay, $ofname, $time_t, $uptorun);
my @injl;

#########
my $exec = "/nfshome0/cmsprod/TransferTest/injection/sendNotification.sh";
########


sub usage
{
  $0 = basename($0);

  die <<EOF;

  Usage $0 --config=<config_file> [--help] [--delay=<delay_between_injections>] [--setulabel=<setuplabel_name>]
          [--time_t=<time_threshold>] [--ofname=<output_script_path>] [--verbose] [--uptorun=<runnumber>]

  Almost all the parameters are obvious. Non-obvious ones are:

	--delay           sets sleeping time before next injection is made (seconds).
			  Default value is 3.

        --time_t          sets minimum time period between file is registered in DAQ
                          DB and execution moment for script to inject only those
                          files that older than value calculated as <localtime>-<delay>.
                          Default value is 60*60*3.

	--ofname	  unless set injection is made in runtime. Sets output filename
			  if immediate execution is not needed but injection bash script
			  generation only.

	--uptorun 	  sets a run number (exlusively) up to which files are being injected.

EOF
}

$help = $debug = 0;
$delay = 1;
$filename = '';
$time_t = 60*60*3;
$uptorun = 0;
GetOptions(
           "help"          => \$help,
           "config=s"      => \$config,
	   "delay=s"	   => \$delay,
	   "setuplabel=s"  => \$setuplabel,
	   "ofname=s"	   => \$ofname,
	   "time_t=s"	   => \$time_t,
	   "verbose"	   => \$verbose,
	   "uptorun=s"	   => \$uptorun
	  );

$help && usage;

die "Configuration file is not set correctly. Use $0 --help for help\n" if (!defined($config));

###
### Script takes all files that have not been copied yet and reinjects them
###

### Query DB subroutine. Fills hash of files to be sent
sub GetFiles {

  my ($sql, $sth);
  my ($where_block, $localtime, $row);

  $localtime = time;


  my $databaseinstance = "DBI:Oracle:cms_rcms";
  my $databasename = "CMS_STOMGR";
  my $databaseuser = "CMS_STOMGR_TIER0_W";
  my $databasepassword = "";

  $sql = "SELECT a.filename,a.hostname,a.setuplabel,a.type,a.stream,";
  $sql .= "a.app_name,a.app_version,a.runnumber,a.lumisection,";
  $sql .= "b.pathname,b.destination,b.nevents,b.filesize,b.checksum,b.comment_str ";
  $sql .= "FROM " . $databasename . ".files_created a ";
  $sql .= "INNER JOIN " . $databasename . ".files_injected b ";
  $sql .= "ON a.filename = b.filename ";
  $sql .= "LEFT OUTER JOIN " . $databasename . ".files_trans_new c ";
  $sql .= "ON a.filename = c.filename ";
  $sql .= "WHERE c.filename IS NULL ";
  $sql .= "AND a.setuplabel = 'GlobalCRAFT1' ";

#  $sql = "SELECT a.filename,a.hostname,a.setuplabel,a.type,a.stream,";
#  $sql .= "a.app_name,a.app_version,a.runnumber,a.lumisection,";
#  $sql .= "b.pathname,b.destination,b.nevents,b.filesize,b.checksum ";
#  $sql .= "FROM " . $databasename . ".files_created a ";
#  $sql .= "INNER JOIN " . $databasename . ".files_injected b ";
#  $sql .= "ON a.filename = b.filename ";
#  $sql .= "INNER JOIN " . $databasename . ".files_trans_new c ";
#  $sql .= "ON a.filename = c.filename ";
#  $sql .= "LEFT OUTER JOIN " . $databasename . ".files_trans_copied d ";
#  $sql .= "ON a.filename = d.filename ";
#  $sql .= "WHERE d.filename IS NULL ";
#  $sql .= "AND a.setuplabel = 'PrivCal' ";
#  $sql .= "AND a.runnumber not in ( 57593, 57787, 57793, 57959, 57960, 57961, 57962, 57965, 57969, 57971, 57979, 58006, 58409, 58462, 58504, 58552 ) ";

#  $sql = "SELECT a.filename,a.hostname,a.setuplabel,a.type,a.stream,";
#  $sql .= "a.app_name,a.app_version,a.runnumber,a.lumisection,";
#  $sql .= "b.pathname,b.destination,b.nevents,b.filesize,b.checksum ";
#  $sql .= "FROM " . $databasename . ".files_created a ";
#  $sql .= "INNER JOIN " . $databasename . ".files_injected b ";
#  $sql .= "ON a.filename = b.filename ";
#  $sql .= "INNER JOIN " . $databasename . ".files_trans_copied c ";
#  $sql .= "ON a.filename = c.filename ";
#  $sql .= "LEFT OUTER JOIN " . $databasename . ".files_trans_checked d ";
#  $sql .= "ON a.filename = d.filename ";
#  $sql .= "WHERE d.filename IS NULL ";
#  $sql .= "AND a.setuplabel = 'GlobalCruzet4MW36' ";
#  $sql .= "AND a.runnumber not in ( 57593, 57787, 57793, 57959, 57960, 57961, 57962, 57965, 57969, 57971, 57979, 58006, 58409, 58462, 58504, 58552 ) ";

#  $sql = "SELECT a.filename,a.hostname,a.setuplabel,a.type,a.stream,";
#  $sql .= "a.app_name,a.app_version,a.runnumber,a.lumisection,";
#  $sql .= "b.pathname,b.destination,b.nevents,b.filesize,b.checksum ";
#  $sql .= "FROM " . $databasename . ".files_created a ";
#  $sql .= "INNER JOIN " . $databasename . ".files_injected b ";
#  $sql .= "ON a.filename = b.filename ";
#  $sql .= "INNER JOIN " . $databasename . ".files_trans_checked c ";
#  $sql .= "ON a.filename = c.filename ";
#  $sql .= "LEFT OUTER JOIN " . $databasename . ".files_trans_inserted d ";
#  $sql .= "ON a.filename = d.filename ";
#  $sql .= "WHERE d.filename IS NULL ";
#  $sql .= "AND a.setuplabel = 'GlobalCruzet4MW36' ";
#  $sql .= "AND a.runnumber not in ( 57593, 57787, 57793, 57959, 57960, 57961, 57962, 57965, 57969, 57971, 57979, 58006, 58409, 58462, 58504, 58552 ) ";

  # connect to database
  my $dbh = DBI->connect($databaseinstance,$databaseuser,$databasepassword);

  unless ( $dbh )
    {
      die "cannot connect to the DB: $DBI::errstr\n";
    };

  $verbose && print "SQL statement to be used is: $sql\n";

  $sth = $dbh->prepare( $sql ) || die "failed prepare : $dbh->errstr\n";
  $sth->execute() || die "failed execute : $dbh->errstr\n";

  my $count = 0;
  while ( my @fd = $sth->fetchrow_array() ) {

    next unless defined($fd[0]);
    next unless defined($fd[1]);
    next unless defined($fd[2]);
    next unless defined($fd[3]);
    next unless defined($fd[4]);
    next unless defined($fd[5]);
    next unless defined($fd[6]);
    next unless defined($fd[7]);
    next unless defined($fd[8]);
    next unless defined($fd[9]);
    next unless defined($fd[10]);
    next unless defined($fd[11]);
    next unless defined($fd[12]);
    next unless defined($fd[14]); # this already has HLTKEY= in database

    $row = "$exec --FILENAME=$fd[0] --HOSTNAME=$fd[1] --SETUPLABEL=$fd[2] --TYPE=$fd[3] --STREAM=$fd[4] --APP_NAME=$fd[5] --APP_VERSION=$fd[6] --RUNNUMBER=$fd[7] --LUMISECTION=$fd[8] --PATHNAME=$fd[9] --DESTINATION=$fd[10] --NEVENTS=$fd[11] --FILESIZE=$fd[12] --$fd[14]";

    if ( $fd[13] )
      {
	$row .= " --CHECKSUM=$fd[13]";
      }

    my $useIndex = 1;
    if ( $useIndex )
      {
        my $indexfile = $fd[0];
        $indexfile =~ s/.dat/.ind/;
        $row .= " --INDEX=$indexfile";
      }

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
#$env = 'export T0_BASE_DIR=/nfshome0/cmsprod/TransferTest
#export T0ROOT=${T0_BASE_DIR}/T0
#export CONFIG=${T0_BASE_DIR}/Config/TransferSystem_Cessy.cfg
#export PERL5LIB=${T0ROOT}/perl_lib:${T0_BASE_DIR}/perl';

if ($ofname)
  {
    open (FILE, ">$ofname") or die "$!\n";

    #print FILE "echo 'Setting environment' \n$env\n\n";

    map {print FILE "echo '$sstr ". (++$count) . " $estr' \n$_\n$sleepstr\n"} @injl;

    close(FILE);
  }
#else
#  {
#    for $_ (@injl)
#      {
#	print "$sstr ". (++$count) . " $estr";
#	$verbose && print "To be executed: $_\n";
#	`$env; $_`;
#	sleep $delay;
#      }
#  }

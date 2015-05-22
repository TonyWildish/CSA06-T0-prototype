echo $*

if (${?PERL5LIB}) then
  setenv PERL5LIB $PWD/perl_lib:${PERL5LIB}
else
  setenv PERL5LIB $PWD/perl_lib
endif

setenv PERL5LIB ${PERL5LIB}:/afs/cern.ch/user/w/wildish/public/perl:/afs/cern.ch/user/w/wildish/public/perl/ApMon_perl-2.2.8

setenv PYTHONPATH /afs/cern.ch/user/s/sinanis/scratch0/DBSInjector/DBS/DBS_0_0_3/COMP/DBS/Clients/PythonAPI 

setenv T0ROOT $PWD


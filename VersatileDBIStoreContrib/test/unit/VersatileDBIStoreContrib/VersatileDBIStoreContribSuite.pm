package VersatileDBIStoreContribSuite;

use Unit::TestSuite;
our @ISA = qw( Unit::TestSuite );

sub name { 'VersatileDBIStoreContribSuite' };

sub include_tests { qw(VersatileDBIStoreContribTests) };

1;

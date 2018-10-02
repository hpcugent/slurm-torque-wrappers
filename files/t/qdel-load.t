use strict;
use warnings;

use Test::More;
use Test::MockModule;

BEGIN {unshift(@INC, '.', 't');}
require 'qdel.pl';

ok(1, "Basic loading ok");

done_testing;

use strict;
use warnings;

use Test::More;
use Test::MockModule;

BEGIN {
    unshift(@INC, '.', 't');
    require 'qalter.pl';
}

my ($update, $tmsg, $test);

# poor mans main mocking
sub qalter_update {
    my ($opts, $msg) = @_;
    $test++;
    $tmsg = '' if ! defined($tmsg);
    $tmsg .= " test $test";

    is($opts->{job_id}, 123, "jobid 123 $tmsg");
    delete $opts->{job_id};

    diag "$tmsg ", explain $opts;
    is_deeply($update, $opts, "qalter update $tmsg");
};

ok(1, "Basic loading ok");


# TODO: uncomment when we know how to implement it
@ARGV = ('-l', 'nodes=2:ppn=5,vmem=10gb', '123', '-l', 'walltime=1:2:3');
$update = {
    'max_nodes' => '2',
    'min_nodes' => '2',
    'ntasks_per_node' => '5',
    'num_tasks' => '10',
    'pn_min_memory' => '10240',
    'time_limit' => '63', # 1 hour 2 minutes; plus ceil of 3 seconds = 1 extra minute
};
qalter_main();

done_testing;

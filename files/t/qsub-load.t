use strict;
use warnings;

use Test::More;
use Test::MockModule;
use DateTime;

BEGIN {
    unshift(@INC, '.', 't');

    *CORE::GLOBAL::exit = sub {die("exit @_")};
}

require 'qsub.pl';

ok(1, "Basic loading ok");

=head1 convert_mb_format

=cut

is(convert_mb_format('2g'), '2048M', 'Convert GB to MB');


=head1 get_miutes

=cut

# any started second is a full minute
is(get_minutes('1:2:0'), 62, 'Convert to minutes');
is(get_minutes('1:2:1'), 63, 'Convert to minutes (ceil up)');

is(get_minutes(1000), 17, 'Convert seconds to minutes');

=head1 parse_resource_list

key/value with undef value are stripped from result

=cut

my %resc = (
    "" => [{},[]],
    "walltime=1,nodes=2,mem=2g" => [{mem => '2048M', nodes => 2, walltime => 1}, [qw(mem nodes walltime)]],
    "walltime=100:5:5,nodes=123:ppn=123" => [{nodes => '123:ppn=123', walltime => 100*60+5+1}, [qw(nodes walltime)]],
    "nodes=123:ppn=123:gpus" => [{nodes => '123:ppn=123:gpus'}, [qw(nodes)]],
    "nodes=124:ppn=124,naccelerators=2" => [{naccelerators => 'gpus:2', nodes => '124:ppn=124'}, [qw(naccelerators nodes)]],
    "nodes=125:ppn=125,gpus=3" => [{naccelerators => 'gpus:3', nodes => '125:ppn=125'}, [qw(gpus nodes)]],
    "gpus=4" => [{naccelerators => 'gpus:4'}, [qw(gpus)]],
    "nodes=126:ppn=126,mps=5" => [{naccelerators => 'mps:5', nodes => '126:ppn=126'}, [qw(mps nodes)]],
    "mps=7" => [{naccelerators => 'mps:7'}, [qw(mps)]],
    );

foreach my $resctxt (sort keys %resc) {
    my ($rsc, $mat) = parse_resource_list($resctxt);
    # sort matches
    $mat = [sort @$mat];
    # strip undefs
    $rsc = {map {$_ => $rsc->{$_}} grep {defined $rsc->{$_}} sort keys %$rsc};
    diag "resource '$resctxt' ", explain $rsc, " matches ", explain $mat;
    is_deeply($rsc, $resc{$resctxt}->[0], "converted rescource list '$resctxt'");
    is_deeply($mat, $resc{$resctxt}->[1], "converted rescource list '$resctxt' matches");
}

foreach my $wrong (qw(nodes=1,ppn=2,gpus nodes=1:ppn=2,gpu gpu gpu=7)) {
    local $@;
    eval {
        parse_resource_list($wrong);
    };
    ok($@, "exit called with message for $wrong");
}

=head1 parse_node_opts

=cut

# the part after nodes=
my %nopts = (
    "1" => {hostlist => undef, node_cnt => 1, task_cnt => 0, nacc => 0},
    "123:ppn=321" => {hostlist => undef, node_cnt => 123, task_cnt => 321, max_ppn => 321, nacc => 0},
    "host1+host2:ppn=3" => {hostlist => undef, node_cnt => 0, task_cnt => 3, max_ppn => 3, nacc => 0}, # TODO: fix this
    "1:gpus" => {hostlist => undef, node_cnt => 1, task_cnt => 0, nacc => "gpus:1"},
    "5:ppn=4:gpus=3" => {hostlist => undef, node_cnt => 5, task_cnt => 4, max_ppn => 4, nacc => "gpus:3"},
    "6:mps" => {hostlist => undef, node_cnt => 6, task_cnt => 0, nacc => "mps:100"},
    "7:ppn=8:mps=9" => {hostlist => undef, node_cnt => 7, task_cnt => 8, max_ppn => 8, nacc => "mps:9"},
    );

foreach my $notxt (sort keys %nopts) {
    my $nodes = parse_node_opts($notxt);
    diag "resource '$notxt' ", explain $nodes;
    is_deeply($nodes, $nopts{$notxt}, "converted node option '$notxt'");
}

foreach my $wrong (qw(1:gpu 2:gpu=4 3:pn=2 4:pppn=7)) {
    local $@;
    eval {
        parse_node_opts($wrong);
    };
    ok($@, "exit called with message for $wrong");
}


=head1 split_variables

=cut

is_deeply(split_variables("x"), {x => undef}, "trivial split");
is_deeply(split_variables("x,y=value,z=,zz=',',xx=1"), {
    x => undef,
    xx => '1',
    y => 'value',
    z => '',
    zz => "','",
}, "more complex split example");

=head1 convert_begin_time

=cut


my $mocktime = Test::MockModule->new('DateTime');
my %opts;
$mocktime->mock('now', sub {
                DateTime->new(year => 2018, month => 11, day=>21, hour=>12, minute => 23, second => 37, %opts);
                });

my $res = {
    'garbage' => ['garbage'], # non torque, assume slurm
    '1600' => ['2018-11-21T16:00:00'],
    '1200' => ['2018-11-22T12:00:00'], # next day
    '1201' => ['2019-01-01T12:01:00', month => 12, day=>31], # next day, which triggers next month, which triggers next year
    '211234', ['2018-11-21T12:34:00'],
    '201234', ['2018-12-20T12:34:00'],  # next month
    '211134', ['2018-12-21T11:34:00'],  # next month, triggered by hour
    '11211234', ['2018-11-21T12:34:00'],
    '11201234', ['2019-11-20T12:34:00'], # next year
    '201811211234', ['2018-11-21T12:34:00'],
    '201811211234.45' => ['2018-11-21T12:34:45'],
    '1811211234.45' => ['2018-11-21T12:34:45'],  # no CC
    '1811211234' => ['2018-11-21T12:34:00'],
};
foreach my $test (sort keys %$res) {
    my @val = @{$res->{$test}};
    my $value = shift(@val);
    %opts = @val;
    is(convert_begin_time($test), $value, "converted $test");
}

{
    local $@;
    eval {
        convert_begin_time('1811201234');
    };
    ok($@, "exit called with date (incl year) in the past)");
}

done_testing;

use strict;
use warnings;

use Test::More;
use Test::MockModule;

BEGIN {unshift(@INC, '.', 't');}
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
    );

foreach my $resctxt (sort keys %resc) {
    my ($rsc, $mat) = parse_resource_list($resctxt);
    # sort matches
    $mat = [sort @$mat];
    # strip undefs
    $rsc = {map {$_ => $rsc->{$_}} grep {defined $rsc->{$_}} sort keys %$rsc};
    diag "resource '$resctxt' ", explain $rsc, " matches ", $mat;
    is_deeply($rsc, $resc{$resctxt}->[0], "converted rescource list '$resctxt'");
    is_deeply($mat, $resc{$resctxt}->[1], "converted rescource list '$resctxt' matches");
}

=head1 parse_node_opts

=cut

# the part after nodes=
my %nopts = (
    "1" => {hostlist => undef, node_cnt => 1, task_cnt => 0},
    "123:ppn=321" => {hostlist => undef, node_cnt => 123, task_cnt => 321, max_ppn => 321},
    "host1+host2:ppn=3" => {hostlist => undef, node_cnt => 0, task_cnt => 3, max_ppn => 3}, # TODO: fix this
    );

foreach my $notxt (sort keys %nopts) {
    my $nodes = parse_node_opts($notxt);
    diag "resource '$notxt' ", explain $nodes;
    is_deeply($nodes, $nopts{$notxt}, "converted node option '$notxt'");
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

done_testing;

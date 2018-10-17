use strict;
use warnings;

use Test::More;
use Test::MockModule;
use Cwd;

my $submitfilter;
BEGIN {
    # poor mans main mocking
    sub find_submitfilter {$submitfilter};

    unshift(@INC, '.', 't');
}

require 'qsub.pl';

my $sbatch = which("sbatch");
my $salloc = which("salloc");


# TODO: mixed order (ie permute or no order); error on unknown options

# key = generated command text (w/o sbatch)
# value = arrayref

# default args
my @da = qw(script arg1 -l nodes=2:ppn=4);
# default batch argument string
my $dba = "--nodes=2 --ntasks=8 --ntasks-per-node=4";
# defaults
my $defs = {
    e => getcwd . '/%x.e%A',
    o => getcwd . '/%x.o%A',
    J => 'script',
    export => 'NONE',
    'get-user-env' => '60L',
    chdir => $ENV{HOME},
};
# default script args
my $dsa = "script arg1";

my %comms = (
    "$dba $dsa", [@da],
    # should be equal
    "$dba --time=1 --mem=1024M $dsa Y", [qw(-l mem=1g,walltime=1), @da, 'Y'],
    "$dba --time=1 --mem=1024M $dsa X", [qw(-l mem=1g -l walltime=1), @da, 'X'],
    "$dba --time=1 --mem=1024M $dsa X", [@da, 'X', qw(-l vmem=1g -l walltime=1)],

    "$dba --mem=2048M $dsa", [qw(-l vmem=2gb), @da],
    "$dba --mem-per-cpu=10M $dsa", [qw(-l pvmem=10mb), @da],
    "$dba --mem-per-cpu=20M $dsa", [qw(-l pmem=20mb), @da],
    "$dba --abc=123 --def=456 $dsa", [qw(--pass=abc=123 --pass=def=456), @da],
    );

=head1 test all commands in %comms hash

=cut

foreach my $cmdtxt (sort keys %comms) {
    my $arr = $comms{$cmdtxt};
    diag "args ", join(" ", @$arr);
    diag "cmdtxt '$cmdtxt'";

    @ARGV = (@$arr);
    my ($mode, $command, $block, $script, $script_args, $defaults) = make_command();
    diag "mode ", $mode || 0;
    diag "interactive ", ($mode & 1 << 1) ? 1 : 0;
    diag "dryrun ", ($mode & 1 << 2) ? 1 : 0;
    diag "command '".join(" ", @$command)."'";
    diag "block '$block'";

    is(join(" ", @$command), "$sbatch $cmdtxt", "expected command for '$cmdtxt'");
    is($script, 'script', "expected script $script for '$cmdtxt'");
    my @expargs = qw(arg1);
    push(@expargs, $1) if $cmdtxt =~ m/(X|Y)$/;
    is_deeply($script_args, \@expargs, "expected scriptargs ".join(" ", @$script_args)." for '$cmdtxt'");
    is_deeply($defaults, $defs, "expected defaults for '$cmdtxt'");
}

=head1 test submitfilter

=cut

# set submitfilter
$submitfilter = "/my/submitfilter";

@ARGV = (@da);
my ($mode, $command, $block, $script, $script_args, $defaults) = make_command($submitfilter);
diag "submitfilter command @$command";
my $txt = "$sbatch $dba";
is(join(" ", @$command), $txt, "expected command for submitfilter");

# no match
$txt .= " -J script --chdir=$ENV{HOME} -e ".getcwd."/%x.e%A --export=NONE --get-user-env=60L -o ".getcwd."/%x.o%A";
my ($newtxt, $newcommand) = parse_script('', $command, $defaults);
is(join(" ", @$newcommand), $txt, "expected command after parse_script without eo");

# replace PBS_JOBID
# no -o/e/J
# insert shebang
{
    local $ENV{SHELL} = '/some/shell';
    my $stdin = "#\n#PBS -l abd -o stdout.\${PBS_JOBID}..\$PBS_JOBID\n#\n#PBS -e /abc -N def\ncmd\n";
    ($newtxt, $newcommand) = parse_script($stdin, $command, $defaults);
    is(join(" ", @$newcommand),
       "$sbatch --nodes=2 --ntasks=8 --ntasks-per-node=4 --chdir=$ENV{HOME} --export=NONE --get-user-env=60L",
       "expected command after parse_script with eo");
    is($newtxt, "#!/some/shell\n#\n#PBS -l abd -o ".getcwd."/stdout.%A..%A\n#\n#PBS -e /abc -N def\ncmd\n",
       "PBS_JOBID replaced");
}

=head1 interactive job

=cut

@ARGV = ('-I', '-l', 'nodes=2:ppn=4', '-l', 'vmem=2gb');
($mode, $command, $block, $script, $script_args, $defaults) = make_command($submitfilter);
diag "interactive command @$command default ", explain $defaults;
$txt = "$dba --mem=2048M srun --pty";
is(join(" ", @$command), "$salloc $txt", "expected command for interactive");
$script =~ s#^/usr##;
is($script, '/bin/bash', "interactive script value is the bash shell command");
is_deeply($script_args, ['-i', '-l'], 'interactive script args');
ok($mode & 1 << 1, "interactive mode");
ok(!($mode & 1 << 2), "no dryrun mode w interactive");
# no 'get-user-env' (neither for salloc where it belongs but requires root; nor srun)
is_deeply($defaults, {
    J => 'INTERACTIVE',
    export => 'USER,HOME',
    'cpu-bind' => 'v,none',
    chdir => $ENV{HOME},
}, "interactive defaults");

# no 'bash -i'
$txt = "$salloc -J INTERACTIVE $txt --chdir=$ENV{HOME} --cpu-bind=v,none --export=USER,HOME";
($newtxt, $newcommand) = parse_script(undef, $command, $defaults);
ok(!defined($newtxt), "no text for interactive job");
is(join(" ", @$newcommand), $txt, "expected command after parse with interactive");


done_testing();

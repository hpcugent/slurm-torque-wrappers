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

my $mocktime = Test::MockModule->new('DateTime');
$mocktime->mock('now', sub {
                DateTime->new(year => 2018, month => 11, day=>21, hour=>12, minute => 23, second => 37);
                });

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
    "$dba --begin=2018-11-21T16:00:00 $dsa", [qw(-a 1600), @da],
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
$txt = "$dba --mem=2048M srun --pty --mem-per-cpu=0";
is(join(" ", @$command), "$salloc $txt", "expected command for interactive");
$script =~ s#^/usr##;
is($script, '/bin/bash', "interactive script value is the bash shell command");
is_deeply($script_args, ['-i', '-l'], 'interactive script args');
ok($mode & 1 << 1, "interactive mode");
ok(!($mode & 1 << 2), "no dryrun mode w interactive");
# no 'get-user-env' (neither for salloc where it belongs but requires root; nor srun)
is_deeply($defaults, {
    J => 'INTERACTIVE',
    export => 'USER,HOME,TERM',
    'cpu-bind' => 'v,none',
    chdir => $ENV{HOME},
}, "interactive defaults");

# no 'bash -i'
$txt = "$salloc -J INTERACTIVE $txt --chdir=$ENV{HOME} --cpu-bind=v,none --export=USER,HOME,TERM";
($newtxt, $newcommand) = parse_script(undef, $command, $defaults);
ok(!defined($newtxt), "no text for interactive job");
is(join(" ", @$newcommand), $txt, "expected command after parse with interactive");

=head1 qsub -d

=cut

my $dir = '/just/a/test';
@ARGV = ('-d', $dir);
($mode, $command, $block, $script, $script_args, $defaults) = make_command($submitfilter);
$txt = "--chdir=$dir";
my $cmdstr = join(' ', @$command);
ok(index($cmdstr, $txt) != -1, "$txt appears in: $cmdstr");
# make sure --chdir is only included once in the generated command (i.e. no more --chdir=$HOME)
my $count = ($cmdstr =~ /--chdir/g);
is($count, 1, "exactly one --chdir found: $count");

=head1 test parse_script for -j directive and if -e/-o directive is a directory

=cut

sub pst
{
    my ($stdin, $static_ARGV) = @_;
    my ($mode, $command, $block, $script, $script_args, $defaults, $destination) = make_command();
    my ($newtxt, $newcommand) = parse_script($stdin, $command, $defaults, $static_ARGV, $destination);
    my $txt = join(' ', @$newcommand);
    return $txt, $newtxt;
}

my $stdin = "#\n#PBS -j oe\ncmd\n";
$txt = " -e ";
($cmdstr, $newtxt) = pst($stdin);
is(index($cmdstr, $txt), -1, "With -j directive, \"$txt\" argument should not be in: $cmdstr");

$stdin = "#\n#PBS -e .\n#PBS -o output\ncmd\n";
$txt = "-e " . getcwd . "/./%";
($cmdstr, $newtxt) = pst($stdin);
isnt(index($cmdstr, $txt), -1, "If -e directive is a directory, \"$txt\" argument should be in: $cmdstr");

$stdin = "#\n#PBS -o .\n#PBS -j oe\ncmd\n";
$txt = "-o " . getcwd . "/./%";
my $txt2 = " -e ";
($cmdstr, $newtxt) = pst($stdin);
isnt(index($cmdstr, $txt), -1, "If -o directive is a directory and -j directive is present, \"$txt\" argument should be in: $cmdstr");
is(index($cmdstr, $txt2), -1, "If -o directive is a directory and -j directive is present, \"$txt2\" argument should not be in: $cmdstr");

$stdin = "";
$txt = "-o " . getcwd . "/./%";
@ARGV = ('-o', '.');
($cmdstr, $newtxt) = pst($stdin);
isnt(index($cmdstr, $txt), -1, "If -o argument is a directory, \"$txt\" argument should be in: $cmdstr");

$txt = "-e " . getcwd . "/./%";
@ARGV = ('-e', '.');
($cmdstr, $newtxt) = pst($stdin);
isnt(index($cmdstr, $txt), -1, "If -e argument is a directory, \"$txt\" argument should be in: $cmdstr");

sub generate
{
    my @array;
    my ($format, $comm_dir) = @_;
    mkdir "dir_${comm_dir}_dir";
    for my $e (" ", "-e dir_${comm_dir}_dir ", "-e $comm_dir ") {
        for my $o (" ", "-o dir_${comm_dir}_dir ", "-o $comm_dir ") {
            for my $j (" ", "-j oe ") {
                for my $N (" ",  "-N ${comm_dir}_name ") {
                    push(@array, sprintf($format, $e, $o, $j, $N));
                };
            };
        };
    };
    return @array
}

sub check_eo_test {
    my ($commandline, $stdin, $cmdstr, $getcwd, $oore) = @_;
    my $outorerr = "Output";
    if ($oore eq "e") {
        $outorerr = "Error";
    }
    my $name_check = sub {
        my $comm_or_std = $commandline;
        my $comm_or_std_txt = "commandline";
        if ($_[0] eq "stdin") {
            $comm_or_std = $stdin;
            $comm_or_std_txt = "directive";
        }
        if (index($comm_or_std, "-$oore dir_${comm_or_std_txt}_dir") != -1) {
            isnt(index($cmdstr, "-$oore $getcwd/dir_${comm_or_std_txt}_dir/"), -1, "$outorerr should be in dir_${comm_or_std_txt}_dir directory\ncommandline: $commandline\nstdin: $stdin\ncmdstr: $cmdstr\n");
            if (index($commandline, "-N") != -1 ) {
                isnt(index($cmdstr, "-$oore $getcwd/dir_${comm_or_std_txt}_dir/commandline_name"), -1, "Name of the file should be taken form command line\ncommandline: $commandline\nstdin: $stdin\ncmdstr: $cmdstr\n");
                is(index($cmdstr, "-$oore $getcwd/dir_${comm_or_std_txt}_dir/directive_name"), -1, "Name of the file should be taken form command line, not from directive\ncommandline: $commandline\nstdin: $stdin\ncmdstr: $cmdstr\n");
            } else {
                if (index($stdin, "-N") != -1 ) {
                isnt(index($cmdstr, "-$oore $getcwd/dir_${comm_or_std_txt}_dir/%x"), -1, "Name of the file should be handled by Slurm\ncommandline: $commandline\nstdin: $stdin\ncmdstr: $cmdstr\n");
                }
            }
        }
    };
    if (index($commandline, "-$oore") != -1) {
        is(index($cmdstr, "-$oore $getcwd/directive"), -1, "If -$oore in commandline defined, then -$oore directive should be ignored\ncommandline: $commandline\nstdin: $stdin\ncmdstr: $cmdstr\n");
        if (index($commandline, "-$oore commandline") != -1) {
            isnt(index($cmdstr, "-$oore $getcwd/commandline"), -1, "$outorerr name should be commandline\ncommandline: $commandline\nstdin: $stdin\ncmdstr: $cmdstr\n");
        }
        &$name_check("commandline");
    } else {
        if (index($stdin, "-$oore directive") != -1) {
            is(index($cmdstr, "-$oore $getcwd/directive"), -1, "$outorerr name handled by slurm plugin\ncommandline: $commandline\nstdin: $stdin\ncmdstr: $cmdstr\n");
        }
        &$name_check("stdin");
    }
}

my @commandlines = generate("%s %s %s %s", "commandline");
my @stdins = generate("#!/bin/bash\n#PBS %s\n#PBS %s\n#PBS %s\n#PBS %s\ncmd\n", "directive");
my $getcwd = getcwd;
for my $commandline (@commandlines) {
    for $stdin (@stdins) {
        my @static_ARGV = split ' ', $commandline;
        @ARGV = @static_ARGV;
        ($cmdstr, $newtxt) = pst($stdin, \@static_ARGV);
        check_eo_test($commandline, $stdin, $cmdstr, $getcwd, "o");
        if (index($commandline, "-j oe") != -1 || index($stdin, "-j oe") != -1) {
            is(index($cmdstr, "-e "), -1, "If -j command line option or directive is defined, -e should not be defined\ncommandline: $commandline\nstdin: $stdin\ncmdstr: $cmdstr\n");
        } else {
            check_eo_test($commandline, $stdin, $cmdstr, $getcwd, "e");
        }
        if (index($commandline, "-N") != -1 ) {
            isnt(index($cmdstr, "-J commandline_name"), -1, "Name should be taken form command line\ncommandline: $commandline\nstdin: $stdin\ncmdstr: $cmdstr\n");
            is(index($cmdstr, "-J directive_name"), -1, "Name should be taken form command line, not from directives\ncommandline: $commandline\nstdin: $stdin\ncmdstr: $cmdstr\n");
        } else {
            if (index($stdin, "-N") != -1 ) {
            is(index($cmdstr, "-J directive_name"), -1, "Name is handled by Slurm\ncommandline: $commandline\nstdin: $stdin\ncmdstr: $cmdstr\n");
            }
        }
    };
};

$stdin = "";
$txt = "--x11";
@ARGV = ('-X');
($cmdstr, $newtxt) = pst($stdin);
isnt(index($cmdstr, $txt), -1, "If -X option used, \"$txt\" option should be in: $cmdstr");

$stdin = "#!/usr/bin/bash\n#PBS -X\necho\n";
$txt = "--x11";
($cmdstr, $newtxt) = pst($stdin);
isnt(index($cmdstr, $txt), -1, "If -X directive used, \"$txt\" option should be in: $cmdstr");

$stdin = "";
$txt = "#PBS -l walltime=72:00:00";
@ARGV = ('-q', 'long');
($cmdstr, $newtxt) = pst($stdin);
isnt(index($newtxt, $txt), -1, "If -q long option used, \"$txt\" directive should be in: $newtxt");

$stdin = "#!/usr/bin/bash\n#PBS -q long\necho\n";
$txt = "#PBS -l walltime=72:00:00";
($cmdstr, $newtxt) = pst($stdin);
isnt(index($newtxt, $txt), -1, "If -q long directive used, \"$txt\" directive should be in: $newtxt");

local $ENV{SLURM_CLUSTERS} = "kluster";
$stdin = "";
$txt = "--partition $ENV{SLURM_CLUSTERS}_special";
@ARGV = ('-q', 'special');
($cmdstr, $newtxt) = pst($stdin);
isnt(index($cmdstr, $txt), -1, "If -q  $ENV{SLURM_CLUSTERS}_special option used, \"$txt\" option should be in: $cmdstr");

$stdin = "#!/usr/bin/bash\n#PBS -q special \necho\n";
$txt = "--partition $ENV{SLURM_CLUSTERS}_special";
($cmdstr, $newtxt) = pst($stdin);
isnt(index($cmdstr, $txt), -1, "If -q $ENV{SLURM_CLUSTERS}_special directive used, \"$txt\" option should be in: $cmdstr");

done_testing();

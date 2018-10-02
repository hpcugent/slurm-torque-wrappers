#!/usr/bin/perl -w
###############################################################################
#
# qsub - submit a batch job in familar pbs/Grid Engine format.
#
#
###############################################################################
#  Copyright (C) 2015-2016 SchedMD LLC
#  Copyright (C) 2007 The Regents of the University of California.
#  Produced at Lawrence Livermore National Laboratory (cf, DISCLAIMER).
#  Written by Danny Auble <da@schedmd.com>.
#  CODE-OCEC-09-009. All rights reserved.
#
#  This file is part of SLURM, a resource management program.
#  For details, see <https://slurm.schedmd.com/>.
#  Please also read the included file: DISCLAIMER.
#
#  SLURM is free software; you can redistribute it and/or modify it under
#  the terms of the GNU General Public License as published by the Free
#  Software Foundation; either version 2 of the License, or (at your option)
#  any later version.
#
#  In addition, as a special exception, the copyright holders give permission
#  to link the code of portions of this program with the OpenSSL library under
#  certain conditions as described in each individual source file, and
#  distribute linked combinations including the two. You must obey the GNU
#  General Public License in all respects for all of the code used other than
#  OpenSSL. If you modify file(s) with this exception, you may extend this
#  exception to your version of the file(s), but you are not obligated to do
#  so. If you do not wish to do so, delete this exception statement from your
#  version.  If you delete this exception statement from all source files in
#  the program, then also delete it here.
#
#  SLURM is distributed in the hope that it will be useful, but WITHOUT ANY
#  WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS
#  FOR A PARTICULAR PURPOSE.  See the GNU General Public License for more
#  details.
#
#  You should have received a copy of the GNU General Public License along
#  with SLURM; if not, write to the Free Software Foundation, Inc.,
#  51 Franklin Street, Fifth Floor, Boston, MA 02110-1301  USA.
#
###############################################################################

use warnings;
use strict;

use FindBin;
use Getopt::Long 2.24 qw(:config no_ignore_case permute);
use lib "${FindBin::Bin}/../lib/perl";
use autouse 'Pod::Usage' => qw(pod2usage);
use Slurm ':all';
use Switch;
use English;
use File::Basename;
use Data::Dumper;
# not in perl, but packaged in every OS
use IPC::Run qw(run);
use List::Util qw(first);
use Carp;
use Cwd;

BEGIN {
    sub which
    {
        my ($bin) = @_;

        if ($bin !~ m{^/}) {
            foreach my $path (split(":", $ENV{PATH} || '')) {
                my $test = "$path/$bin";
                if (-x $test) {
                    $bin = $test;
                    last;
                }
            }
        }

        return $bin;
    }
}

use constant SBATCH => which("sbatch");
use constant SALLOC => which("salloc");

use constant INTERACTIVE => 1 << 1;
use constant DRYRUN => 1 << 2;

use constant TORQUE_CFGS => qw(/var/spool/pbs/torque.cfg /var/spool/torque/torque.cfg);
use constant DEFAULT_SHELL => '/bin/bash';

# there's some strange bug that resets HOME and USER and TERM
#   it's probably safe to export them, they are fixed
use constant INTERACTIVE_MISSING_VARS => qw(USER HOME TERM);

# Global debug flag
my $debug;

sub report_txt
{
    my $txt = join(" ", map {ref($_) eq '' ? $_ : Dumper($_)} grep {defined($_)} @_);
    $txt =~ s/\n+$//;
    return "$txt\n";
}

sub debug
{
    if ($debug) {
        print "DEBUG: ".report_txt(@_);
    }
}

sub fatal
{
    print "ERROR: ".report_txt(@_);
    exit 1;
}


sub find_submitfilter
{
    # look for torque.cfg in /var/spool/pbs or torque
    my $sf;
    foreach my $cfg (TORQUE_CFGS) {
        next if ! -f $cfg;
        # only check first match
        open(my $fh, '<', $cfg)
            or fatal("Could not open torque cfg file '$cfg' $!");

        while (my $row = <$fh>) {
            $sf = $1 if $row =~ m/^SUBMITFILTER\s+(\/.*?)\s*$/;
        }
        close $fh;
        last;
    }
    debug($sf ? "Found submitfilter $sf" : "No submitfilter found");
    return $sf;
}


# fake: some mocked interactive mode, used in qalter
sub make_command
{
    my ($sf, $fake) = @_;
    my (
        $start_time,
        $account,
        $array,
        $err_path,
        $export_env,
        $interactive,
        $hold,
        $join_output,
        @resource_list,
        $mail_options,
        $mail_user_list,
        $job_name,
        $out_path,
        @pe_ev_opts,
        $priority,
        $requeue,
        $destination,
        $dryrun,
        $variable_list,
        @additional_attributes,
        $wckey,
        $workdir,
        $wrap,
        $help,
        $resp,
        $man,
        @pass
        );

    GetOptions(
        'a=s'      => \$start_time,
        'A=s'      => \$account,
        'b=s'      => \$wrap,
        'cwd'      => sub { }, # this is the default
        'e=s'      => \$err_path,
        'h'        => \$hold,
        'I'        => \$interactive,
        'j:s'      => \$join_output,
        'J=s'      => \$array,
        'l=s'      => \@resource_list,
        'm=s'      => \$mail_options,
        'M=s'      => \$mail_user_list,
        'N=s'      => \$job_name,
        'o=s'      => \$out_path,
        'p=i'      => \$priority,
        'pe=s{2}'  => \@pe_ev_opts,
        'P=s'      => \$wckey,
        'q=s'      => \$destination,
        'r=s'      => \$requeue,
        'S=s'      => sub { warn "option -S is ignored, " .
                                "specify shell via #!<shell> in the job script\n" },
        't=s'      => \$array,
        'v=s'      => \$variable_list,
        'V'        => \$export_env,
        'wd=s'     => \$workdir,
        'W=s'      => \@additional_attributes,
        'help|?'   => \$help,
        'man'      => \$man,
        'sbatchline|dryrun' => \$dryrun,
        'debug|D'      => \$debug,
        'pass=s' => \@pass,
        )
        or pod2usage(2);

    # Display usage if necessary
    pod2usage(0) if $help;
    if ($man) {
        if ($< == 0) {   # Cannot invoke perldoc as root
            my $id = eval { getpwnam("nobody") };
            $id = eval { getpwnam("nouser") } unless defined $id;
            $id = -2                          unless defined $id;
            $<  = $id;
        }

        $> = $<;                         # Disengage setuid
        $ENV{PATH} = "/bin:/usr/bin";    # Untaint PATH
        delete @ENV{'IFS', 'CDPATH', 'ENV', 'BASH_ENV'};
        if ($0 =~ /^([-\/\w\.]+)$/) {
            # Untaint $0
            $0 = $1;
        } else {
            fatal("Illegal characters were found in \$0 ($0)");
        }
        pod2usage(-exitstatus => 0, -verbose => 2);
    }

    # Use sole remaining argument as jobIds
    my ($script, @script_args, $script_cmd, $defaults);
    my $mode = 0;

    $mode |= DRYRUN if $dryrun;

    # torque defaults to start from homedir
    $defaults->{chdir} = $ENV{HOME};

    if ($ARGV[0]) {
        $script = shift(@ARGV);
        $defaults->{J} = basename($script) if ! $job_name;
        @script_args = (@ARGV);
        $script_cmd = join(" ", $script, @script_args);
    } else {
        $defaults->{J} = "sbatch" if ! $job_name;
    }

    my $block = 0;
    my ($depend, $group_list, $res_opts, $node_opts);

    # remove PBS_NODEFILE environment as passed in to qsub.
    if ($ENV{PBS_NODEFILE}) {
        delete $ENV{PBS_NODEFILE};
    }

    # Process options provided with the -W name=value syntax.
    my $W;
    foreach $W (@additional_attributes) {
        my ($name, $value) = split('=', $W);
        if ($name eq 'umask') {
            $ENV{SLURM_UMASK} = $value;
        } elsif ($name eq 'depend') {
            $depend = $value;
        } elsif ($name eq 'group_list') {
            $group_list = $value;
        } elsif (lc($name) eq 'block') {
            if (defined $value) {
                $block = $value eq 'true' ? 1 : 0;
            }
        }
    }

    ($res_opts, $node_opts) = parse_all_resource_list(@resource_list)
        if @resource_list;

    if (@pe_ev_opts) {
        my %pe_opts = %{parse_pe_opts(@pe_ev_opts)};

        # From Stanford: This parallel environment is designed to support
        # applications that use pthreads to manage multiple threads with
        # access to a single pool of shared memory.  The SGE PE restricts
        # the slots used to a threads on a single host, so in this, I think
        # it is equivalent to the --cpus-per-task option of sbatch.
        $res_opts->{mppdepth} = $pe_opts{shm} if $pe_opts{shm};
    }

    # The (main) command
    my @command;
    # The interactive sub-command (is added to regular command in both cases)
    my @intcommand;

    if ($interactive || $fake) {
        $mode |= INTERACTIVE;
        @command = (which(SALLOC));
        @intcommand = (which('srun'), '--pty');
        $defaults->{J} = "INTERACTIVE" if exists($defaults->{J});
        $defaults->{'cpu-bind'} = 'v,none';

        # Always want at least one node in the allocation
        if (!$node_opts->{node_cnt} && !$fake) {
            $node_opts->{node_cnt} = 1;
        }

        # Calculate the task count based of the node cnt and the amount
        # of ppn's in the request
        if ($node_opts->{task_cnt} && (!$fake || $node_opts->{node_cnt})) {
            $node_opts->{task_cnt} *= $node_opts->{node_cnt};
        }
    } else {
        @command = (which(SBATCH));

        if (!$join_output) {
            if ($err_path) {
                $err_path = getcwd . "/err_path" if $err_path !~ m{^/};
                push(@command, "-e", $err_path);
            } else {
                # jobname will be forced
                my $path = getcwd . "/%x.e%A";
                $path .= ".%a" if $array;
                $defaults->{e} = $path;
            }
        }

        if ($out_path) {
            $out_path = getcwd . "/out_path" if $out_path !~ m{^/};
            push(@command, "-o", $out_path);
        } else {
            # jobname is forced
            my $path = getcwd . "/%x.o%A";
            $path .= ".%a" if $array;
            $defaults->{o} = $path;
        }

        # The job size specification may be within the batch script,
        # Reset task count if node count also specified
        if ($node_opts->{task_cnt} && $node_opts->{node_cnt}) {
            $node_opts->{task_cnt} *= $node_opts->{node_cnt};
        }
    }

    # job_name is always passed, because sbatch calls happen with stdin redirected
    push(@command, "-J", $job_name) if $job_name;

    push(@command, "--nodes=$node_opts->{node_cnt}") if $node_opts->{node_cnt};
    push(@command, "--ntasks=$node_opts->{task_cnt}") if $node_opts->{task_cnt};
    push(@command, "--nodelist=$node_opts->{hostlist}") if $node_opts->{hostlist};

    push(@command, "--chdir=$workdir") if $workdir;

    push(@command, "--mincpus=$res_opts->{ncpus}") if $res_opts->{ncpus};
    push(@command, "--ntasks-per-node=$res_opts->{mppnppn}")  if $res_opts->{mppnppn};

    my $time;
    if ($res_opts->{walltime}) {
        $time = $res_opts->{walltime};
    } elsif ($res_opts->{cput}) {
        $time = $res_opts->{cput};
    } elsif($res_opts->{pcput}) {
        $time = $res_opts->{pcput};
    }
    push(@command, "--time=$time") if defined($time);

    if ($variable_list) {
        my $vars = split_variables($variable_list);

        my @vars;
        if ($interactive) {
            @vars = $export_env ? qw(all) : INTERACTIVE_MISSING_VARS;

            # also set the values
            foreach my $var (grep {defined($vars->{$_})} sort keys %$vars) {
                $ENV{$var} = $vars->{$var};
            }
        } else {
            @vars = ($export_env ? 'all' : 'none');
        }

        push(@vars, (map {$_.(defined($vars->{$_}) ? "=$vars->{$_}" : "")} sort keys %$vars));
        # command is execute in non-subshell, so no magic quting required
        push(@intcommand, "--export=".join(',', @vars));
    } elsif ($export_env) {
        push(@intcommand, "--export=all");
    } else {
        if ($interactive) {
            $defaults->{export} = join(',', INTERACTIVE_MISSING_VARS);
            # salloc with get-user-env requires user to be root
        } else {
            $defaults->{export} = 'NONE';
            $defaults->{"get-user-env"} = '60L';
        }
    }

    push(@command, "--account=$group_list") if $group_list;
    push(@command, "--array=$array") if $array;
    push(@command, "--constraint=$res_opts->{proc}") if $res_opts->{proc};
    push(@command, "--dependency=$depend")   if $depend;
    push(@command, "--tmp=$res_opts->{file}")  if $res_opts->{file};

    if ($res_opts->{mem} && ! $res_opts->{pmem}) {
        push(@command, "--mem=$res_opts->{mem}");
    } elsif ($res_opts->{pmem} && ! $res_opts->{mem}) {
        push(@command, "--mem-per-cpu=$res_opts->{pmem}");
    } elsif ($res_opts->{pmem} && $res_opts->{mem}) {
        fatal("Both mem and pmem defined");
    }
    push(@command, "--nice=$res_opts->{nice}") if $res_opts->{nice};

    push(@command, "--gres=gpu:$res_opts->{naccelerators}") if $res_opts->{naccelerators};

    # Cray-specific options
    push(@command, "--ntasks=$res_opts->{mppwidth}") if $res_opts->{mppwidth};
    push(@command, "--nodelist=$res_opts->{mppnodes}") if $res_opts->{mppnodes};
    push(@command, "--cpus-per-task=$res_opts->{mppdepth}") if $res_opts->{mppdepth};

    push(@command, "--begin=$start_time") if $start_time;
    push(@command, "--account=$account") if $account;
    push(@command, "-H") if $hold;

    if ($mail_options) {
        push(@command, "--mail-type=FAIL") if $mail_options =~ /a/;
        push(@command, "--mail-type=BEGIN") if $mail_options =~ /b/;
        push(@command, "--mail-type=END") if $mail_options =~ /e/;
        push(@command, "--mail-type=NONE") if $mail_options =~ /n/;
    }
    push(@command, "--mail-user=$mail_user_list") if $mail_user_list;
    push(@command, "--nice=$priority") if $priority;
    push(@command, "-p", $destination) if $destination;
    push(@command, "--wckey=$wckey") if $wckey;

    if ($requeue) {
        if ($requeue =~ 'y') {
            push(@command, "--requeue");
        } elsif ($requeue =~ 'n') {
            push(@command, "--no-requeue");
        }
    }

    push(@command, map {"--$_"} @pass);

    # join command and intcommand
    push(@command, @intcommand);

    if ($interactive) {
        # whatever is run by srun is not part of the command
        # allows to add defaults
        $script = which('bash');
        # use -l because there is no get-user-env
        @script_args = ('-i', '-l');
    } elsif ($script) {
        if ($wrap && $wrap =~ 'y') {
            if ($sf) {
                fatal("Cannot wrap with submitfilter enabled");
            } else {
                push(@command, "--wrap=$script_cmd");
            }
        } else {
            if (!$sf) {
                push(@command, $script, @script_args);
            }
        }
    }

    return $mode, \@command, $block, $script, \@script_args, $defaults;
}


# split comma-sep list of variables (optional value) like x,y=value,z=,zz=","
# return hashref; undef value means no value was set
sub split_variables
{
    my ($txt) = @_;

    my @vars = split(qr{,(?=\w+(?:,|=|$))}, $txt);
    # use m/^(\w+)(?:=(['"]?)(.*)\2)?$/ to remove quote
    # but regular quotes will not get passed due to the shell parser
    return {map {$_ =~ m/^(\w+)(?:=(.*))?$/; $1 => $2} grep {m/^\w+/} @vars};
}

sub run_submitfilter
{
    my ($sf, $script, $args) = @_;

    my ($stdin, $stdout, $stderr);

    # Read whole script, so we can do some preprocessing of our own?
    my $fh;
    if ($script) {
        open($fh, '<', $script);
    } else {
        $fh = \*STDIN;
    }
    while (<$fh>) {
        $stdin .= $_;
    }
    close($fh);

    local $@;
    eval {
        run([$sf, @$args], \$stdin, \$stdout, \$stderr);
    };

    my $child_exit_status = $CHILD_ERROR >> 8;
    if ($@) {
        fatal("Something went wrong when calling submitfilter: $@");
    }
    print STDERR $stderr if $stderr;

    if ($child_exit_status != 0) {
        fatal("Submitfilter exited with non-zero $child_exit_status");
    }

    return $stdout;
}


sub parse_script
{
    my ($txt, $command, $defaults) = @_;

    my @cmd = @$command;
    my $newtxt;

    my @lines;
    @lines = split("\n", $txt) if defined $txt;

    # insert shebang
    if (defined($txt) && (!@lines || $lines[0] !~ m/^#!/)) {
        # no shebang, insert SHELL
        $newtxt .= "#!".($ENV{SHELL} || DEFAULT_SHELL)."\n";
    }

    # replace PBS_JOBID in -o / -e
    #   torque sets stdout/err in cwd -> so force abspath like done above
    # All script changes here
    foreach my $line (@lines) {
        if ($line =~ m/^\s*(#PBS.*?\s-[oe])(?:\s*(\S+)(.*))?$/) {
            if ($2 !~ m{^/}) {
                $line = "$1 ".getcwd."/$2$3";
            }

            $line =~ s/\$\{?PBS_JOBID\}?/%A/g;
        }

        $newtxt .= "$line\n";
    };


    # Look for PBS directives for -o, -e, -N
    # If they are not set, add the commandline args from defaults
    # keys are slurm option names
    my %map = (
        N => ['J'],
        V => ['export', 'get-user-env'],
        );
    my %set;
    foreach my $line (@lines) {
        last if $line !~ m/^\s*(#|$)/;
        # oset and eset on separate line, in case -e and -o are on same line,
        # mixed with otehr opts etc etc
        foreach my $pbsopt (qw(e o N)) {
            my $opts = $map{$pbsopt} || [$pbsopt];
            my $pat = '^\s*#PBS.*?\s-'.$pbsopt.'\s+\S';
            if ($line =~ m/$pat/) {
                foreach my $opt (@$opts) {
                    $set{$opt} = 1
                };
            }
        }
    };

    # slurm short options
    # add any defaults that are not set
    foreach my $sopt (sort keys %$defaults) {
        if (!$set{$sopt}) {
            my @cmds;
            if (length($sopt) >= 2) {
                @cmds = ("--$sopt=$defaults->{$sopt}");
            } else {
                @cmds = ("-$sopt", $defaults->{$sopt});
            }

            if ($cmd[0] eq SALLOC &&
                grep {$sopt eq $_} ('get-user-env', 'J')) {
                # in interactive mode
                # option for salloc, not for srun
                # whatever is run by srun is not part of the command
                @cmd = ($cmd[0], @cmds, @cmd[1..$#cmd])
            } else {
                push(@cmd, @cmds);
            }
        };
    };

    return ($newtxt, \@cmd);
}

sub main
{
    # copy the arguments; ARGV is modified when getopt parses it
    my @orig_args = (@ARGV);

    my $sf = find_submitfilter;

    my ($mode, $command, $block, $script, $script_args, $defaults) = make_command($sf);

    my $stdin;
    if (!($mode & INTERACTIVE)) {
        my $stdout;

        if ($script && !-f $script) {
            fatal("No jobscript $script");
        }

        if ($sf) {
            # script or no script
            $stdin = run_submitfilter($sf, $script, \@orig_args);
        } elsif ($script) {
            open(my $fh, '<', $script);
            while (<$fh>) {
                $stdin .= $_;
            }
            close($fh);
        } else {
            # read from input
            while (<STDIN>) {
                $stdin .= $_;
            }

            fatal ("No script and nothing from stdin") if !$stdin;
        }
    }

    # stdin is not relevant for interactive jobs
    # but should also add the defaults
    ($stdin, $command) = parse_script($stdin, $command, $defaults);

    # Execute the command and capture its stdout, stderr, and exit status.
    # Note that if interactive mode was requested,
    # the standard output and standard error are _not_ captured.
    if ($mode & INTERACTIVE) {
        # add script and script_args
        push(@$command, $script, @$script_args);

        # TODO: fix issues with space in options; also use IPC::Run
        my $command_txt = join(" ", @$command);
        my $ret = 0;
        if ($mode & DRYRUN) {
            print "$command_txt\n";
        } else {
            debug("Generated interactive", ($block ? ' blocking' : undef), " command '$command_txt'");
            $ret = system($command_txt);
        }
        exit ($ret >> 8);
    } else {

        # TODO: fix issues with space in options
        #   the actual code execution is done without subshell , so no worries there
        my $command_txt = join(" ", @$command);
        if ($mode & DRYRUN) {
            print "$command_txt\n";
            exit 0;
        } else {
            debug("Generated", ($block ? 'blocking' : undef), "command '$command_txt'");
        }

        my $stdout;

        local $@;
        eval {
            # Execute the command and capture the combined stdout and stderr.
            # TODO: why is this required?
            run($command, \$stdin, '>&', \$stdout);
        };
        my $command_exit_status = $CHILD_ERROR >> 8;
        if ($@) {
            fatal("Something went wrong when calling sbatch: $@");
        }

        # If available, extract the job ID from the command output and print
        # it to stdout, as done in the PBS version of qsub.
        if ($command_exit_status == 0) {
            my ($job_id) = $stdout =~ m/(\d+)(?:\s+on\s+cluster\s+.*?)?\s*$/;
            debug("Got output $stdout");
            print "$job_id\n";

            # If block is true wait for the job to finish
            if ($block) {
                my $slurm = Slurm::new();
                if (!$slurm) {
                    fatal("Problem loading slurm.");
                }
                sleep 2;
                my ($job) = $slurm->load_job($job_id);
                my $resp = $$job{'job_array'}[0]->{job_state};
                while ( $resp < JOB_COMPLETE ) {
                    $job = $slurm->load_job($job_id);
                    $resp = $$job{'job_array'}[0]->{job_state};
                    sleep 1;
                }
            }
        } else {
            print "There was an error running the SLURM sbatch command.\n" .
                  "The command was:\n'".join(" ", @$command)."'\n" .
                  "and the output was:\n'$stdout'\n";
        }


        # Exit with the command return code.
        exit($command_exit_status >> 8);
    }
}

sub parse_resource_list
{
    my ($rl) = @_;
    my %opt = (
        'accelerator' => "",
        'arch' => "",
        'block' => "",
        'cput' => "",
        'file' => "",
        'host' => "",
        'h_rt' => "",
        'h_vmem' => "",
        'mem' => "",
        'mpiprocs' => "",
        'ncpus' => "",
        'nice' => "",
        'nodes' => "",
        'naccelerators' => "",
        'opsys' => "",
        'other' => "",
        'pcput' => "",
        'pmem' => "",
        'proc' => '',
        'pvmem' => "",
        'select' => "",
        'software' => "",
        'vmem' => "",
        'walltime' => "",
        # Cray-specific resources
        'mppwidth' => "",
        'mppdepth' => "",
        'mppnppn' => "",
        'mppmem' => "",
        'mppnodes' => "",
        );
    my @keys = keys(%opt);

    # The select option uses a ":" separator rather than ","
    # This wrapper currently does not support multiple select options

    # Protect the colons used to separate elements in walltime=hh:mm:ss.
    # Convert to NNhNNmNNs format.
    $rl =~ s/(walltime|h_rt)=(\d+):(\d{1,2}):(\d{1,2})/$1=$2h$3m$4s/;

    # TODO: why is this here? breaks e.g. :ppn=... structure
    #$rl =~ s/:/,/g;

    my @matches;
    foreach my $key (@keys) {
        ($opt{$key}) = $rl =~ m/\b$key=([\w:.=+]+)/;
        push(@matches, $key) if defined($opt{$key});
    }

    if ($opt{h_rt} && !$opt{walltime}) {
        $opt{walltime} = $opt{h_rt};
        push(@matches, 'walltime');
    }

    # If needed, un-protect the walltime string.
    if ($opt{walltime}) {
        $opt{walltime} =~ s/(\d+)h(\d{1,2})m(\d{1,2})s/$1:$2:$3/;
        # Convert to minutes for SLURM.
        $opt{walltime} = get_minutes($opt{walltime});
    }

    if ($opt{accelerator} &&
        $opt{accelerator} =~ /^[Tt]/ &&
        !$opt{naccelerators}) {
        $opt{naccelerators} = 1;
        push(@matches, 'naccelerators');
    }

    if ($opt{cput}) {
        $opt{cput} = get_minutes($opt{cput});
    }

    if ($opt{mpiprocs} &&
        (!$opt{mppnppn} || ($opt{mpiprocs} > $opt{mppnppn}))) {
        $opt{mppnppn} = $opt{mpiprocs};
        push(@matches, 'mppnppn');
    }

    if ($opt{vmem}) {
        debug ("mem and vmem specified; forcing vmem value") if $opt{mem};
        $opt{mem} = $opt{vmem};
        push(@matches, 'mem');
    }

    if ($opt{pvmem}) {
        debug ("pmem and pvmem specified; forcing pvmem value") if $opt{pmem};
        $opt{pmem} = $opt{pvmem};
        push(@matches, 'pmem');
    }

    $opt{pmem} = convert_mb_format($opt{pmem}) if $opt{pmem};

    if ($opt{h_vmem}) {
        # Transfer over the GridEngine value (no conversion)
        $opt{mem} = $opt{h_vmem};
        push(@matches, 'mem');
    } elsif ($opt{mppmem}) {
        $opt{mem} = convert_mb_format($opt{mppmem});
        push(@matches, 'mem');
    } elsif ($opt{mem}) {
        $opt{mem} = convert_mb_format($opt{mem});
    }

    if ($opt{file}) {
        $opt{file} = convert_mb_format($opt{file});
    }

    return \%opt, \@matches;
}

sub parse_all_resource_list
{
    my (@resource_list) = @_;

    my ($res_opts, $node_opts);
    foreach my $rl (@resource_list) {
        my ($opts, $matches) = parse_resource_list($rl);
        # Loop over all values, how to determine that a value is not reset with default option?
        if ($res_opts && %$res_opts) {
            # only set/update matches
            foreach my $key (@$matches) {
                $res_opts->{$key} = $opts->{$key};
            }
        } else {
            # nothing done yet, set all values, incl defaults/undef
            $res_opts = $opts;
        }
    }

    if ($res_opts->{nodes}) {
        $node_opts = parse_node_opts($res_opts->{nodes});
    }
    if ($res_opts->{select} && (!$node_opts->{node_cnt} || ($res_opts->{select} > $node_opts->{node_cnt}))) {
        $node_opts->{node_cnt} = $res_opts->{select};
    }
    if ($res_opts->{select} && $res_opts->{ncpus} && $res_opts->{mpiprocs}) {
        my $cpus_per_task = int ($res_opts->{ncpus} / $res_opts->{mppnppn});
        if (!$res_opts->{mppdepth} || ($cpus_per_task > $res_opts->{mppdepth})) {
            $res_opts->{mppdepth} = $cpus_per_task;
        }
    }
    if ($node_opts->{max_ppn} && ! $res_opts->{mppnppn}) {
        $res_opts->{mppnppn} = $node_opts->{max_ppn};
    }

    return ($res_opts, $node_opts);
}


sub parse_node_opts
{
    my ($node_string) = @_;
    my %opt = (
        'node_cnt' => 0,
        'hostlist' => "",
        'task_cnt' => 0
        );
    my $max_ppn;
    while ($node_string =~ /ppn=(\d+)/g) {
        $opt{task_cnt} += $1;
        $max_ppn = $1 if !$max_ppn || ($1 > $max_ppn);
    }

    $opt{max_ppn} = $max_ppn if defined $max_ppn;

    my $hl = Slurm::Hostlist::create("");

    my @parts = split(/\+/, $node_string);
    foreach my $part (@parts) {
        my @sub_parts = split(/:/, $part);
        foreach my $sub_part (@sub_parts) {
            if ($sub_part =~ /ppn=(\d+)/) {
                next;
            } elsif ($sub_part =~ /^(\d+)/) {
                $opt{node_cnt} += $1;
            } else {
                if (!Slurm::Hostlist::push($hl, $sub_part)) {
                    print "problem pushing host $sub_part onto hostlist\n";
                }
            }
        }
    }

    $opt{hostlist} = Slurm::Hostlist::ranged_string($hl);

    my $hl_cnt = Slurm::Hostlist::count($hl);
    $opt{node_cnt} = $hl_cnt if $hl_cnt > $opt{node_cnt};

    return \%opt;
}

sub parse_pe_opts
{
    my (@pe_array) = @_;
    my %opt = (
        'shm' => 0,
        );
    my @keys = keys(%opt);

    foreach my $key (@keys) {
        $opt{$key} = $pe_array[1] if ($key eq $pe_array[0]);
    }

    return \%opt;
}

sub get_minutes
{
    my ($duration) = @_;
    $duration = 0 unless $duration;
    my $minutes = 0;

    # Convert [[HH:]MM:]SS to duration in minutes
    if ($duration =~ /^(?:(\d+):)?(\d*):(\d+)$/) {
        my ($hh, $mm, $ss) = ($1 || 0, $2 || 0, $3);
        $minutes += 1 if $ss > 0;
        $minutes += $mm;
        $minutes += $hh * 60;
    } elsif ($duration =~ /^(\d+)$/) {  # Convert number in minutes to seconds
        my $mod = $duration % 60;
        $minutes = int($duration / 60);
        $minutes++ if $mod;
    } else { # Unsupported format
        fatal("Invalid time limit specified ($duration)");
    }

    return $minutes;
}

sub convert_mb_format
{
    my ($value) = @_;
    my ($amount, $suffix) = $value =~ /(\d+)($|[KMGT])b?/i;
    return if !$amount;
    $suffix = lc($suffix);

    if (!$suffix) {
        $amount /= 1048576;
    } elsif ($suffix eq "k") {
        $amount /= 1024;
    } elsif ($suffix eq "m") {
        #do nothing this is what we want.
    } elsif ($suffix eq "g") {
        $amount *= 1024;
    } elsif ($suffix eq "t") {
        $amount *= 1048576;
    } else {
        print "don't know what to do with suffix $suffix\n";
        return;
    }

    $amount .= "M";

    return $amount;
}

# Run main
main() unless caller;

##############################################################################

__END__

=head1 NAME

B<qsub> - submit a batch job in a familiar PBS format

=head1 SYNOPSIS

qsub  [-a start_time]
      [-A account]
      [-b y|n]
      [-e err_path]
      [-I]
      [-l resource_list]
      [-m mail_options] [-M user_list]
      [-N job_name]
      [-o out_path]
      [-p priority]
      [-pe shm task_cnt]
      [-P wckey]
      [-q destination]
      [-r y|n]
      [-v variable_list]
      [-V]
      [-wd workdir]
      [-W additional_attributes]
      [-h]
      [--debug|-D]
      [--pass]
      [script]

=head1 DESCRIPTION

The B<qsub> submits batch jobs. It is aimed to be feature-compatible with PBS' qsub.

=head1 OPTIONS

=over 4

=item B<-a>

Earliest start time of job. Format: [HH:MM][MM/DD/YY]

=item B<-A account>

Specify the account to which the job should be charged.

=item B<-b y|n>

Whether to wrap the command line or not

=item B<-e err_path>

Specify a new path to receive the standard error output for the job.

=item B<-I>

Interactive execution. Starts a bash session on the node.

=item B<-J job_array>

Job array index values. The -J and -t options are equivalent.

=item B<-l resource_list>

Specify an additional list of resources to request for the job.

=item B<-m mail_options>

Specify a list of events on which email is to be generated.

=item B<-M user_list>

Specify a list of email addresses to receive messages on specified events.

=item B<-N job_name>

Specify a name for the job.

=item B<-o out_path>

Specify the path to a file to hold the standard output from the job.

=item B<-p priority>

Specify the priority under which the job should run.

=item B<-pe shm cpus-per-task>

Specify the number of cpus per task.

=item B<-P wckey>

Specify the wckey or project of a job.

=item B<-r y|n>

Whether to allow the job to requeue or not.

=item B<-t job_array>

Job array index values. The -J and -t options are equivalent.

=item B<-v> [variable_list]

Export only the specified environment variables. This option can also be used
with the -V option to add newly defined environment variables to the existing
environment. The variable_list is a comma delimited list of existing environment
variable names and/or newly defined environment variables using a name=value
format.

=item B<-V>

The -V option to exports the current environment, which is the default mode of
options unless the -v option is used.

=item B<-wd workdir>

Specify the workdir of a job.  The default is the current work dir.

=item B<-?> | B<--help>

Brief help message

=item B<--man>

Full documentation

=item B<-D> | B<--debug>

Report some debug information, e.g. the actual SLURM command.

=item B<--dryrun>

Only print the command, do not actually run.

=item B<--pass>

Passthrough for args to the C<sbatch>/C<salloc> command. One or more C<--pass>
options form an long-option list, the leading C<--> are prefixed;
e.g. C<--pass=constraint=alist> will add C<--constraint=alist>.

Short optionnames are not supported. Combine multiple C<pass> options to pass
multiple options; do not contruct one long command string.

=back

=cut

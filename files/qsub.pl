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
use DateTime;
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
        $x11forward,
        $help,
        $resp,
        $man,
        @pass,
        $gpus,
        $cpus_per_gpu,
        $mem_per_gpu,
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
        'wd|d=s'   => \$workdir,
        'W=s'      => \@additional_attributes,
        'X'        => \$x11forward,
        'help|?'   => \$help,
        'man'      => \$man,
        'sbatchline|dryrun' => \$dryrun,
        'debug|D'      => \$debug,
        'pass=s' => \@pass,

        # slurm gpu options (gpu-per-node is already supported via noderesource :gpus=X (-> --gres=gpu=X))
        # TODO: directives support, incl in submitfilter? (#PBS --slurm-option --> #SBATCH --slurm-option)
        'gpus|G=i' => \$gpus,
        'cpus-per-gpu=i' => \$cpus_per_gpu,
        'mem-per-gpu=s' => \$mem_per_gpu,
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
        } elsif ($name eq 'x') {
            if ($value =~ m/ADVRES:([^\r\n\t\f\v "]+)/i) {
                $ENV{SBATCH_RESERVATION} = $1;
            }
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
        if ($script) {
            fatal("Interactive jobs are not allowed to run a jobscript (in this case: \"$script\")");
        }
        if ($array) {
            fatal("Interactive job can not be job array");
        }
        $mode |= INTERACTIVE;
        @command = (which(SALLOC));
        @intcommand = (which('srun'), '--pty', '--mpi=none');
        $defaults->{J} = "INTERACTIVE" if exists($defaults->{J});
        $defaults->{'cpu-bind'} = 'none';

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

        my $fix_path = sub {
            my ($eopath, $ext) = @_;
            if ($eopath && ! -d $eopath) {
                $eopath = getcwd . "/$eopath" if $eopath !~ m{^/};
                push(@command, "-" . $ext, $eopath);
            } else {
                # jobname will be forced
                my $jobbasename = $job_name ? basename($job_name) : "%x";
                my $path = ($eopath || getcwd) . "/$jobbasename.$ext%A";
                $path = getcwd . "/$path" if $path !~ m{^/};
                $defaults->{$ext} = $path;
            }

        };
        if (!$join_output) {
            &$fix_path($err_path, "e");
        }

        &$fix_path($out_path, "o");

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

    push(@command, "--mincpus=$res_opts->{ncpus}") if $res_opts->{ncpus};
    push(@command, "--ntasks-per-node=$res_opts->{mppnppn}")  if $res_opts->{mppnppn};
    push(@command, '--x11') if $x11forward;

    if ($workdir) {
        push(@command, "--chdir=$workdir");
    } else {
        # torque defaults to start from homedir
        $defaults->{chdir} = $ENV{HOME};
    }

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
        push(@intcommand, '--mem=0') if $interactive;
    } elsif ($res_opts->{pmem} && ! $res_opts->{mem}) {
        push(@command, "--mem-per-cpu=$res_opts->{pmem}");
        push(@intcommand, '--mem-per-cpu=0') if $interactive;
    } elsif ($res_opts->{pmem} && $res_opts->{mem}) {
        fatal("Both mem and pmem defined");
    }
    push(@command, "--nice=$res_opts->{nice}") if $res_opts->{nice};

    # TODO: support all/half for both -l gpus and --gpus
    my $has_gpu;
    if ($res_opts->{naccelerators}) {
        my ($acc, $num) = split(':', $res_opts->{naccelerators});
        if ($gpus) {
            fatal("You cannot define both :$acc=X/-l $acc=X node resource and the --gpus option")
        } else {
            my $gresacc = $acc;
            $gresacc = 'gpu' if $gresacc eq "gpus";
            push(@command, "--gres=$gresacc:$num");
            # For now, until magic meaning of gpus=0 is equal that of mem=0, allow all gpus in interactive shell
            #push(@intcommand, "--gres=$gresacc:0") if $interactive;
        }

        if ($cpus_per_gpu) {
            fatal("--cpus-per-gpu requires --gpus option (and thus conflicts with :$acc/-l $acc")
        }

        $has_gpu = 1;
    } elsif ($gpus) {
        push(@command, "--gpus=$gpus");
        push(@intcommand, '--gpus=0') if $interactive;
        # apparently, only when --gpus is set (according to man page)
        push(@command, "--cpus-per-gpu=$cpus_per_gpu") if $cpus_per_gpu;
        # no equivalent for interactive?

        $has_gpu = 1;
    }

    # TODO: handle node resource GPUs too. might/will conflict with node resourfces such as vmem etc etc
    #       needs support in submitfilter too?
    #       This is a way too simple first attempt. needs to be understood how slurm handles these combos
    my $add_mempergpu = $gpus || ($res_opts->{naccelerators} && ! ($res_opts->{mem} || $res_opts->{pmem}));
    push(@command, "--mem-per-gpu=".convert_mb_format($mem_per_gpu)) if $mem_per_gpu && $add_mempergpu;
    push(@intcommand, "--mem-per-gpu=0") if $interactive && $has_gpu;

    # Cray-specific options
    push(@command, "--ntasks=$res_opts->{mppwidth}") if $res_opts->{mppwidth};
    push(@command, "--nodelist=$res_opts->{mppnodes}") if $res_opts->{mppnodes};
    push(@command, "--cpus-per-task=$res_opts->{mppdepth}") if $res_opts->{mppdepth};

    push(@command, "--begin=".convert_begin_time($start_time)) if $start_time;
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

    return $mode, \@command, $block, $script, \@script_args, $defaults, $destination;
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
    my ($txt, $command, $defaults, $orig_args, $destination) = @_;

    my @cmd = @$command;
    my @newtxt;

    my @lines;
    @lines = split("\n", $txt) if defined $txt;

    # insert shebang
    if (defined($txt) && (!@lines || $lines[0] !~ m/^#!/)) {
        # no shebang, insert SHELL
        push(@newtxt, "#!".($ENV{SHELL} || DEFAULT_SHELL));
    }

    # replace_script_var changes environmental variables in the lines of submitted script starts with #PBS
    # for example (see @replace_script_vars array)
    # "#PBS -m $PBS_O_MAIL" to "#PBS -m e@mail.to", if $MAIL has the value "e@mail.to" at submit time.
    my $replace_env_var = sub {
        my ($line, $script_var, $env_var) = @_;
        if ($line =~ m/^\s*#PBS.*?\$\{?$script_var\}?/ && $ENV{$env_var}) {
            $line =~ s/\$\{?$script_var\}?/$ENV{$env_var}/g;
        }
        return ($line);
    };

    my %replace_script_vars = (
        PBS_O_HOME => 'HOME',
        PBS_O_HOST => 'HOST',
        PBS_O_LOGNAME => 'LOGNAME',
        PBS_O_MAIL => 'MAIL',
        PBS_O_PATH => 'PATH',
        PBS_O_SHELL => 'SHELL',
        PBS_O_WORKDIR => 'PWD',
    );
    #Add all local varibales to @replace_script_vars
    foreach my $existing_env_vars (sort keys %ENV) {
        $replace_script_vars{$existing_env_vars} = $existing_env_vars;
    };

    # Replace env_vars in the submit script (only in #PBS lines)
    foreach my $line (@lines) {
        foreach my $replace_script_var (sort keys %replace_script_vars) {
            $line = &$replace_env_var($line, $replace_script_var, $replace_script_vars{$replace_script_var});
        };
        # replace PBS_JOBID in -o / -e
        #   torque sets stdout/err in cwd -> so force abspath like done above
        if ($line =~ m/^\s*(#PBS.*?\s-[oe])(?:\s*(\S+)(.*))?$/) {
            if ($2 !~ m{^/}) {
                $line = "$1 ".getcwd."/$2$3";
            }

            $line =~ s/\$\{?PBS_JOBID\}?/%A/g;
        }

        if ($line !~ m/^\s*#PBS.*?\s-q/) {
            push(@newtxt, $line);
        }
    };

    # Look for PBS directives o, -e, -N, -j if they not given in the command line
    # If they are not set, add the commandline args from defaults
    # keys are slurm option names
    # Check for -X and -q as well.
    my %orig_argsh = map { s/^-//; $_ => 1 } grep {m/^-.$/} @$orig_args;  # only map the one-letter options, and remove the leading -
    my @check_pbsopt = qw(j o N X q t W);
    push (@check_pbsopt, 'e') if !$orig_argsh{'j'};
    @check_pbsopt = grep {!$orig_argsh{$_}} @check_pbsopt;
    my %map = (
        N => ['J'],
        V => ['export', 'get-user-env'],
        );
    my %set;
    foreach my $line (@lines) {
        last if $line !~ m/^\s*(#|$)/;
        # oset and eset on separate line, in case -e and -o are on same line,
        # mixed with otehr opts etc etc
        foreach my $pbsopt (@check_pbsopt) {
            my $opts = $map{$pbsopt} || [$pbsopt];
            my $pat = $pbsopt eq 'X' ? '^\s*#PBS.*?\s-('.$pbsopt.')' : '^\s*#PBS.*?\s-'.$pbsopt.'\s+(\S+)\s*';
            if ($line =~ m/$pat/) {
                foreach my $opt (@$opts) {
                    $set{$opt} = $1
                };
            }
        }
    };

    # handle queues and special queues (as a partition)
    my $userqueue = $destination || $set{'q'};
    if ($userqueue) {
        if ($userqueue eq "special" ) {
            if (defined $ENV{SLURM_CLUSTERS} && $ENV{SLURM_CLUSTERS} !~ m/[;:,\/]/)  {
                splice(@cmd, 1, 0, '--partition', "$ENV{SLURM_CLUSTERS}_$userqueue");
            } else {
                fatal("We do not support your environment.\n" .
                    "Your environmental variable SLURM_CLUSTERS is\n" .
                    "either empty or contains multiple clusters.\n");
            }
        } else {
            warn "Please do not set a queue (\'-q\' option or \'#PBS -q\' directive).\n" .
                "Right now it sets only the default walltime based on the queue,\n" .
                "So please request explicitly the desired walltime.\n";
        }
        my %modwalltime = (
            'short'   => '1:00:00',
            'bshort'  => '1:00:00',
            'long'    => '72:00:00',
            'debug'   => '15:00',
            'special' => '48:00:00',
        );
        if ($modwalltime{$userqueue}) {
            if (!$orig_argsh{'I'}) {
                splice(@newtxt, 1, 0, "## walltime from deprecated or special queue $userqueue", "#PBS -l walltime=$modwalltime{$userqueue}");
            }
        } else {
            fatal("You have used a non-existing queue name!\n");
        }
    }

    # add x11 forward
    push(@cmd, '--x11') if $set{'X'};

    # add reservation
    if (defined ($set{'W'}) && $set{'W'} =~ m/x(?ii)="?ADVRES:([^\r\n\t\f\v "]+)/) {
        push(@cmd, '--reservation', $1)
    }

    # if -j PBS directive is in the script,
    # do not use default error path for slurm
    my @check_eo = qw(e o);
    if ($set{'j'}) {
        delete $defaults->{e};
        # delete command line defined error file, if -j directive defined
        for (my $element=0; $element < scalar(@cmd); $element++) {
            if ($cmd[$element] eq '-e') {
                splice @cmd, $element, 2;
            }
        }
        @check_eo = ('o');
    }

    # check wheter the -o and -e directives are directory
    # if yes, then set the path for slurm.
    foreach my $dir (@check_eo) {
        unless (grep (/^-$dir$/, @cmd)) {
            if ($set{$dir}) {
                if (-d $set{$dir}) {
                    my $fname = $defaults->{$dir};
                    $fname =~s /\S*(\/\S*)/$1/s;
                    push(@cmd, ("-$dir", $set{$dir}.$fname));
                }
            }
        }
    }

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

    # add array extensions if it is an array job
    if ($orig_argsh{t} || $set{t}) {
        my $arrayext = '-%a';
        for my $element (0 .. $#cmd) {
            foreach my $dir (qw(e o)) {
                if ($cmd[$element] eq "-$dir" && $cmd[$element+1] !~ m{%a}) {
                    $cmd[$element+1] .= $arrayext;
                }
            }
        }
        for my $element (0 .. $#newtxt) {
            if ($newtxt[$element] =~ m/^\s*(#PBS.*?\s-[oe])(?:\s*(\S+)(.*))?$/ && $newtxt[$element] !~ m{%a}) {
                $newtxt[$element] = "$1$2$arrayext$3";
            }
        }
    }

    return (@newtxt ? join("\n", @newtxt, "") : undef, \@cmd);
}

sub main
{
    # copy the arguments; ARGV is modified when getopt parses it
    my @orig_args = (@ARGV);

    my $sf = find_submitfilter;

    my ($mode, $command, $block, $script, $script_args, $defaults, $destination) = make_command($sf);

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
    ($stdin, $command) = parse_script($stdin, $command, $defaults, \@orig_args, $destination);

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
            print ("stdin is:\n--START-OF-STDIN--\n$stdin\n--END-OF-STDIN--\n");
            exit 0;
        } else {
            debug("Generated", ($block ? 'blocking' : undef), "command '$command_txt'");
        }

        my $stdout;

        local $@;
        debug("stdin is:\n--START-OF-STDIN--\n$stdin\n--END-OF-STDIN--\n");
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
        'gpus' => "",  # alias for naccelerators
        'mps' => "",  # sort-of alias for naccelerators
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

    if ($opt{naccelerators}) {
        # these are gpus
        $opt{naccelerators} = "gpus:$opt{naccelerators}"
    }

    if ($opt{gpus} && ($opt{naccelerators} || $opt{mps})) {
        fatal("You cannot specify both gpus and ".($opt{mps} ? 'mps' : 'naccelerators')." as a node resource")
    }

    if ($opt{mps} && ($opt{naccelerators} || $opt{gpus})) {
        fatal("You cannot specify both mps and ".($opt{gpus} ? 'gpus' : 'naccelerators')." as a node resource")
    }

    if ($opt{gpus}) {
        # alias for naccelerators
        $opt{naccelerators} = "gpus:" . (delete $opt{gpus});
    }

    if ($opt{mps}) {
        # sort-of alias for naccelerators
        $opt{naccelerators} = "mps:" . (delete $opt{mps});
    }

    if ($opt{accelerator} &&
        $opt{accelerator} =~ /^[Tt]/ &&
        !$opt{naccelerators}) {
        $opt{naccelerators} = "gpus:1";
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

    my %multiplier = (
        half => 0.5,
        all  => 1,
        );

    my $have_numproc = 0;
    my $tnumproc = 0;
    if ($ENV{VSC_INSTITUTE_CLUSTER}) {
        $tnumproc = qx(python -c "from vsc.jobs.pbs.clusterdata import CLUSTERDATA; print CLUSTERDATA['$ENV{VSC_INSTITUTE_CLUSTER}']['NP']" 2>&1);
        $have_numproc = 1 if !$?;
    }

    foreach my $rl (@resource_list) {
        if ($have_numproc) {
            while ($rl =~ m/ppn=(all|half)/) {
                my $np = int($tnumproc * $multiplier{$1});
                $rl =~ s/(ppn=)$1/$1$np/;
            };
        };

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
        my $nacc = delete $node_opts->{nacc};
        # do not override the ,naccelerators=X with the :gpus=X
        $res_opts->{naccelerators} = $nacc if $nacc && ! defined($res_opts->{naccelerators});
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
        'task_cnt' => 0,
        'nacc' => 0,
        );
    my $max_ppn;
    while ($node_string =~ /ppn=(\d+)/g) {
        $opt{task_cnt} += $1;
        $max_ppn = $1 if !$max_ppn || ($1 > $max_ppn);
    }

    while ($node_string =~ /(gpus|mps)(?:=(\d+))?/g) {
        if ($opt{nacc}) {
            fatal("No support for (mixed) number of $1 over multiple nodes");
        } else {
            $opt{nacc} = "$1:" . (defined($2) ? "$2" : ($1 eq 'mps' ? 100 : 1));  # set 100 MPS default vs 1 gpu
        }
    }

    $opt{max_ppn} = $max_ppn if defined $max_ppn;

    my $hl = Slurm::Hostlist::create("");

    my @parts = split(/\+/, $node_string);
    foreach my $part (@parts) {
        my @sub_parts = split(/:/, $part);
        foreach my $sub_part (@sub_parts) {
            if ($sub_part =~ /(ppn=\d+|(gpus|mps)(=\d+)?)/) {
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
    if (($opt{hostlist} || '') =~ m{ppn=(all|half)}) {
        fatal("Cannot determine number of processors for ppn={all,half}");
    }

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


sub convert_begin_time
{
    my $time = shift;

    # assume correct slurm syntax otherwise

    if ($time =~ m/^
        ( # DD
          ( # MM
            ( # YY
                (?P<CC>\d{2}+)? # CC
              (?P<YY>\d{2})
            )?
            (?P<MM>\d{2})
            )?
          (?P<DD>\d{2})
        )?
        (?P<hh>\d{2})
        (?P<mm>\d{2})
        (?:\.(?P<SS>\d{2}))?
        $/x) {
        my $now = DateTime->now(time_zone => 'local');

        my $date = DateTime->now(time_zone => 'local');
        $date->set_hour($+{hh});
        $date->set_minute($+{mm});
        $date->set_second($+{SS} || 0);

        my $gran = 'days';
        if (defined($+{DD})) {
            $gran = 'months';
            $date->set_day($+{DD});
        }
        if (defined($+{MM})) {
            $gran = 'years';
            $date->set_month($+{MM});
        }

        if (defined($+{YY})) {
            my $yy = $+{YY};
            # FYI, no next century crap
            $gran = undef;
            my $year = $now->year();
            if (defined($+{CC})) {
                $year = "$+{CC}$yy"
            } else {
                # destroys $+
                $year =~ s/\d{2}$/$yy/;
            }
            $date->set_year($year);
        }

        my $delta = $date->subtract_datetime($now);
        if ($delta->is_negative()) {
            if ($gran) {
                $date->add($gran => 1);
            } else {
                fatal("Specified a date in past (wrong year/century): $date");
            }
        }

        # slurm format
        $time = $date->strftime("%Y-%m-%dT%H:%M:%S")
    }

    return $time;
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
      [-d workdir]
      [-e err_path]
      [-I]
      [-l resource_list]
      [-m mail_options] [-M user_list]
      [-N job_name]
      [-o out_path]
      [-p priority]
      [-pe shm task_cnt]
      [-P wckey]
      [-r y|n]
      [-v variable_list]
      [-V]
      [-wd workdir]
      [-W additional_attributes]
      [-X]
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

=item B<-d path>

The working directory path to be used for the job.

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

=item B<-X>

Enables X11 forwarding.

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

=item B<-G> | B<--gpus>

Count of GPUs required for the job. This does not garantee anything wrt the number of GPUs per node,
or the total number of nodes. If you want e.g. X GPUs on one node, specify C<-l gpus=X>
(as the node resource, with 1 node as default number of nodes per job),
or more generic X GPUs on Y nodes  C<-l nodes=Y,gpus=X>
(or equivalent C<-l nodes=Y:gpus=X>).

=item B<--cpus-per-gpu>

Number of CPUs required per allocated GPU (via the C<-G>/C<--gpus> option).

=item B<--mem-per-gpu>

Memory required per allocated GPU (via the C<-G>/C<--gpus> option).

=back

=cut

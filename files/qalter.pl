#! /usr/bin/perl -w
###############################################################################
#
# qalter - PBS wrapper for changing job status using scontrol
#
###############################################################################

use strict;
use warnings;

use FindBin;
use Getopt::Long 2.24 qw(:config no_ignore_case);
use lib "${FindBin::Bin}/../lib/perl";
use autouse 'Pod::Usage' => qw(pod2usage);
use Slurm ':all';
use Slurmdb ':all'; # needed for getting the correct cluster dims
use Switch;

my $qalter = __FILE__;

# shared namespace with qsub
sub qalter_main
{
    my (
        $new_name,
        @resource_list,
        $output,
        $rerun,
        $man,
        $help
        );

    # Parse Command Line Arguments
    GetOptions(
        'N=s'    => \$new_name,
        'r=s'    => \$rerun,
        'o=s'    => \$output,
        'l=s'    => \@resource_list,
        'help|?' => \$help,
        'man'    => \$man
        )
        or pod2usage(2);

    pod2usage(0) if $help;

    if ($man) {
        if ($< == 0) {    # Cannot invoke perldoc as root
            my $id = eval { getpwnam("nobody") };
            $id = eval { getpwnam("nouser") } unless defined $id;
            $id = -2              unless defined $id;
            $<  = $id;
        }
        $> = $<;            # Disengage setuid
        $ENV{PATH} = "/bin:/usr/bin";    # Untaint PATH
        delete @ENV{'IFS', 'CDPATH', 'ENV', 'BASH_ENV'};
        if ($0 =~ /^([-\/\w\.]+)$/) {
            $0 = $1;        # Untaint $0
        } else {
            die "Illegal characters were found in \$0 ($0)\n";
        }
        pod2usage(-exitstatus => 0, -verbose => 2);
    }

    # Check input arguments
    if (@ARGV < 1) {
        pod2usage(-message => "Missing Job ID", -verbose => 0, -exitstatus => 1);
    }

    my $job_id = $ARGV[0];

    my $slurm = Slurm::new();
    if (!$slurm) {
        die "Problem loading slurm.\n";
    }

    my $resp = $slurm->get_end_time($job_id);
    if (not defined($resp)) {
        pod2usage(
            -message => "Job id $job_id not valid!",
            -verbose => 0,
            -exitstatus => 153,
            );
    }
    if ((not defined($new_name)) and (not defined($rerun)) and (not defined($output)) and (not @resource_list)) {
        pod2usage(
            -message => "no argument given!",
            -verbose => 0,
            );
    }

    my %update = (
        job_id => $job_id,
        );

    # Use Slurm's Perl API to change name of a job
    if ($new_name) {
        $update{name} = $new_name;
        qalter_update(\%update, 'name')
    }

    # Use Slurm's Perl API to change the requeue job flag
    if ($rerun) {
        $update{requeue} = (($rerun eq "n") || ($rerun eq "N")) ? 1 : 0;
        qalter_update(\%update, 'requeue')
    }

    # Use Slurm's Perl API to change Comment string
    # Comment is used to relocate an output log file
    if ($output) {
        # Example:
        # $comment="on:16337,stdout=/gpfsm/dhome/lgerner/tmp/slurm-16338.out,stdout=~lgerner/tmp/new16338.out";
        #
        my $comment;
        # Get current comment string from job_id
        my ($job) = $slurm->load_job($job_id);
        $comment = $$job{'job_array'}[0]->{comment};

        # Split at stdout
        if ($comment) {
            my(@outlog) = split("stdout", $comment);

            # Only 1 stdout argument add a ','
            if ($#outlog < 2) {
                $outlog[1] .= ","
            }

            # Add new log file location to the comment string
            $outlog[2] = "=".$output;
            $comment = join("stdout", @outlog);
        } else {
            $comment = "stdout=$output";
        }

        # Make sure that "%j" is changed to current $job_id
        $comment =~ s/%j/$job_id/g ;

        # Update comment and print usage if there is a response
        $update{comment} = $comment;
        qalter_update(\%update, 'comment')
    }

    if (@resource_list) {
        # give it to make_command from qsub as interactive job
        local @ARGV = ('-I', (map {('-l', $_)} @resource_list));

        # do not look at the next lines...
        my $qsub = $qalter;
        $qsub =~ s/qalter/qsub/;
        require "$qsub";

        # use fake=1 to avouid default nodes
        my ($mode, $command, $block, $script, $script_args, $defaults) = make_command(undef, 1);

        # extract all long options from command
        my $longopts = {map {$_ =~ m/^--([\w-]+)=(.*)/; $1 => $2} grep {m/^--[\w-]+=/} @$command};

        # for certain attributes, convert the values
        my $convert = {
            'mem' => sub {my $mem = shift; $mem =~ s/M$//; return $mem},
            'time' => sub {my $time = shift; die("time $time not valid. contact developers") if $time !~ m/^\d+$/; return $time; },
        };

        my $converted = {map {$_ => exists($convert->{$_}) ? $convert->{$_}->($longopts->{$_}) : $longopts->{$_} } sort keys %$longopts};

        my $update;
        # map all names to job info struct attrs
        # see src/scontrol/update_job.c to map scontrol names to attributes
        #   see also src/sbatch/opt.c for mapping of sbatch names to job attributes
        my $attrmap = {
            # MinMemoryNode
            'mem' => ['pn_min_memory'],
            # TODO: look for MinMemoryCPU, which is the same, but with correction for number of cpus/cores
            # NumNodes / ReqNodes
            'nodes' => ['min_nodes', 'max_nodes'],
            # Numtasks / ReqProcs
            'ntasks' => ['num_tasks'],
            #  TasksPerNode
            'ntasks-per-node' => ['ntasks_per_node'],
            # TimeLimit (attrinbute in minutes)
            'time' => ['time_limit'],
        };

        foreach my $key (sort keys %$converted) {
            my $map = $attrmap->{$key};
            if ($map) {
                foreach my $nkey (@$map) {
                    $update->{$nkey} = $converted->{$key};
                }
            } else {
                # ? warn? debug? die?
            }
        }

        # at the end, add job_id
        $update->{job_id} = $job_id;
        qalter_update($update, 'resource_list')
    };
}


sub qalter_update
{
    my ($opts, $msg) = @_;

    if (Slurm->update_job($opts)) {
        my $err = Slurm->get_errno();
        if ($err == ESLURM_REQUESTED_PART_CONFIG_UNAVAILABLE) {
            # Requested partition configuration not available now (2015)
            # --> job (still) won't start, but modification was made
            # not reporting anything wrong
        } else {
            my $resp = Slurm->strerror($err);
            pod2usage(
                -message => "Job id $opts->{job_id} $msg change error: $resp",
                -verbose => 0,
                -exitstatus => 1,
                );
        }
    }
}

# Run main
qalter_main() unless caller;


##############################################################################

__END__

=head1 NAME

B<qalter> - alter a job name, the job rerun flag or the job output file name.

=head1 SYNOPSIS

qalter [-N Name]
       [-r y|n]
       [-o output file]
       <job ID>

=head1 DESCRIPTION

The B<qalter> updates job name, job rerun flag or job output(stdout) log location.

It is aimed to be feature-compatible with PBS' qsub.

=head1 OPTIONS

=over 4

=item B<-N>

Update job name in the queue

=item B<-r>

Alter a job rerunnable flag. "y" will allow a qrerun to be issued. "n" disable qrerun option.

=item B<-o>

Alter a job output log file name (stdout).

The job log will be move/rename after the job has B<terminated>.

=item B<-l>

Alter a job resources.

=item B<-?> | B<--help>

brief help message

=item B<-man>

full documentation

=back

=head1 SEE ALSO

qrerun(1) qsub(1)
=cut

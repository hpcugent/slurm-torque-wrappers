package Slurm;

use strict;
use warnings;

# Minimal Slurm package.

use constant JOB_COMPLETE => 'JOB_COMPLETE';
use constant SLURM_SUCCESS => 'SLURM_SUCCESS';
use constant SLURM_ERROR => 'SLURM_ERROR';

use constant SLURM_TRANSITION_STATE_NO_UPDATE => 'ESLURM_TRANSITION_STATE_NO_UPDATE';
use constant SLURM_JOB_PENDING => 'ESLURM_JOB_PENDING';
use constant SLURM_ALREADY_DONE => 'ESLURM_ALREADY_DONE';
use constant SLURM_INVALID_JOB_ID => 'ESLURM_INVALID_JOB_ID';

use constant ESLURM_REQUESTED_PART_CONFIG_UNAVAILABLE => 'ESLURM_REQUESTED_PART_CONFIG_UNAVAILABLE';

use parent qw(Exporter);
our @EXPORT_OK = qw(JOB_COMPLETE
    SLURM_SUCCESS SLURM_ERROR
    ESLURM_TRANSITION_STATE_NO_UPDATE ESLURM_JOB_PENDING
    ESLURM_ALREADY_DONE ESLURM_INVALID_JOB_ID
    ESLURM_REQUESTED_PART_CONFIG_UNAVAILABLE
);
our %EXPORT_TAGS = (
    all => \@EXPORT_OK,
    );

sub new {
    my $self = {};
    bless $self, 'Slurm';
    return $self;
};

sub get_end_time {1;};

sub load_job {
    return {job_array => [{comment => 'acomment'}]};
}

package Slurm::Hostlist;

use strict;
use warnings;


sub create {};

sub push {};

sub ranged_string {};

sub count {0};



1;

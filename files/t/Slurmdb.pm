package Slurmdb;

use strict;
use warnings;

# Minimal Slurmdb package.

use constant SHOW_ALL => 'SHOW_ALL';
use constant SHOW_DETAIL => 'SHOW_DETAIL';

use constant JOB_STATE_BASE => 'JOB_STATE_BASE';
use constant JOB_COMPLETE => 'JOB_COMPLETE';
use constant JOB_CANCELLED => 'JOB_CANCELLED';
use constant JOB_TIMEOUT => 'JOB_TIMEOUT';
use constant JOB_NODE_FAIL => 'JOB_NODE_FAIL';
use constant JOB_PREEMPTED => 'JOB_PREEMPTED';
use constant JOB_BOOT_FAIL => 'JOB_BOOT_FAIL';
use constant JOB_FAILED => 'JOB_FAILED';
use constant JOB_RUNNING => 'JOB_RUNNING';
use constant JOB_PENDING => 'JOB_PENDING';
use constant JOB_SUSPENDED => 'JOB_SUSPENDED';

use constant INFINITE => 'INFINITE';

use parent qw(Exporter);
our @EXPORT_OK = qw(SHOW_ALL SHOW_DETAIL
    JOB_STATE_BASE JOB_COMPLETE JOB_CANCELLED JOB_TIMEOUT
    JOB_NODE_FAIL JOB_PREEMPTED JOB_BOOT_FAIL JOB_FAILED
    JOB_RUNNING JOB_PENDING JOB_SUSPENDED
    INFINITE
);
our %EXPORT_TAGS = (
    all => \@EXPORT_OK,
    );

1;

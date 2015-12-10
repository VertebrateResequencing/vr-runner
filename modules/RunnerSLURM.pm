=head1 NAME

RunnerMPM.pm   - Runner pipeline framework for SLURM (Simple Linux Utility for Resource Management)

=head1 SYNOPSIS

All Runner pipelines can be run without modification in the SLURM environment.
Just add "+js slurm" or "+js SLURM" to your runner command. For a documentation
about the necessary minimum to extend this framework to other batch job
schedulers, see RunnerMPM.

=head1 METHODS

=cut

package RunnerSLURM;

use strict;
use warnings;
use Carp;
use Runner;

sub new
{
    my ($class,@args) = @_;
    my $self = @args ? {@args} : {};
    bless $self, ref($class) || $class;

    $$self{Unknown} = 0;
    $$self{Running} = 1;
    $$self{Error}   = 2;
    $$self{Done}    = 4;

    $$self{slurm_status} =
    {
        PENDING      => $$self{Running},
        RUNNING      => $$self{Running},
        SUSPENDED    => $$self{Running},
        COMPLETING   => $$self{Running},
        CONFIGURING  => $$self{Running},
        PREEMPTED    => $$self{Running},
        CANCELLED    => $$self{Error},
        FAILED       => $$self{Error},
        TIMEOUT      => $$self{Error},
        NODE_FAIL    => $$self{Error},
        SPECIAL_EXIT => $$self{Errro},
        COMPLETED    => $$self{Done},
    };

    $$self{limits}{max_jobs} = $self->_ncpu();

    return $self;
}

sub throw
{
    my ($self,@msg) = @_;
    confess(@msg);
}

sub _ncpu
{
    my ($self) = @_;
    open(my $fh,'<',"/proc/cpuinfo") or $self->throw("/proc/cpuinfo: $!");
    my $ncpu = 0;
    while (my $line=<$fh>)
    {
        if ( $line=~/^processor/ ) { $ncpu++; }
    }
    close($fh);
    return $ncpu;
}

sub init_jobs
{
    my ($self, $job_name, $ids) = @_;
    my $jids_file = "$job_name.jid";

    # For each input job id create a hash with info: status, number of failuers
    if ( ! -e $jids_file ) 
    { 
        my @jobs_out = ();
        for my $id (@$ids) { push @jobs_out, {status=>$$self{Unknown}, nfailures=>0}; }
        return \@jobs_out; 
    }

    my $jobs  = $self->_read_jobs($job_name);
    my $dirty = 0;
    my @jobs_out = ();
    for my $id (@$ids)
    {
        my $status = 0;
        my $nfails = 0;
        if ( exists($$jobs{Running}{$id}) )
        {
            $dirty += $self->_update_job_state($job_name,$jobs,$id);
        }
        if ( exists($$jobs{Running}{$id}) ) { $status = $$self{Running}; }
        elsif ( exists($$jobs{Done}{$id}) ) { $status = $$self{Done}; }
        elsif ( exists($$jobs{Error}{$id}) ) { $status = $$self{Error}; $nfails = $$jobs{Error}{$id}{nfailures}; }
        push @jobs_out, {status=>$status, nfailures=>$nfails};
    }
    delete($$self{cached_squeue});
    if ( $dirty ) { $self->_write_jobs($job_name,$jobs); }
    return \@jobs_out;
}

# find out the job's status and slurm job id - only the array index
# (runner's id) and slurm array id is known
sub _update_job_state
{
    my ($self,$job_name,$jobs,$id) = @_;
    if ( !exists($$self{cached_squeue}) )
    {
        # Best scenario: The job info is still available via squeue, we do not
        # need glob() to learn about slurm job id nor sacct to learn the state.
        my @lines = `squeue -h -o '%K %F %A %T'`;
        for my $line (@lines)
        {
            my ($array_idx,$array_id,$job_id,$state) = split(/\s+/,$line);
            chomp($state);
            $$self{cached_squeue}{$array_id}{$array_idx} = { job_id=>$job_id, state=>$state };
        }
    }

    # At this point we learned everything we could from squeue. If anything is missing,
    # we need to consult status files and sacct history.

    my $array_id = $$jobs{Running}{$id}{array_id};
    my $dirty = 0;

    if ( $$jobs{Running}{$id}{job_id}==-1 )
    {
        # Don't know about the SLURM job id yet, we need to glob()
        my @files = glob("$job_name.status.$id.$array_id.*");
        for my $file (@files)
        {
            if ( !($file=~/\.(\d+)$/) ) { $self->throw("Could not parse $job_name.status.$id.$array_id.* .. $file\n"); }
            my $job_id = $1;
            $$self{cached_squeue}{$array_id}{$id}{job_id} = $job_id;
            unlink($file);
        }
    }

    if ( !exists($$self{cached_squeue}{$array_id}{$id}{job_id}) ) { $self->throw("No info about $job_name $array_id $id??"); }
    my $job_id = $$self{cached_squeue}{$array_id}{$id}{job_id};

    if ( !exists($$self{cached_squeue}{$array_id}{$id}{state}) )
    {
        # The status is not known, the job is not queued or running. Use sacct to learn the status
        my @state = `sacct -n -j $job_id.batch -o state`;
        if ( !scalar @state ) { $self->throw("No info about $job_name.status.$id.$array_id.*, no output from `sacct -n -j $job_id.batch -o state`\n"); }
        if ( @state>1 ) { $self->throw("Too many lines from `sacct -n -j $job_id.batch -o state`:\n".join('',@state)); }
        chomp($state[0]);
        $$self{cached_squeue}{$array_id}{$id}{state} = $state[0];
    }
    my $state = $$self{cached_squeue}{$array_id}{$id}{state};
    if ( !exists($$self{slurm_status}{$state}) ) { $self->throw("Unknown SLURM status: $state $job_name $array_id $id\n"); }

    if ( $$self{slurm_status}{$state} eq $$self{Running} )
    {
        $$jobs{Running}{$id}{job_id} = $job_id;
        $dirty = 1;
    }
    elsif ( $$self{slurm_status}{$state} eq $$self{Done} )
    {
        $$jobs{Done}{$id} = $$jobs{Running}{$id};
        delete($$jobs{Running}{$id});
        $dirty = 1;
    }
    elsif ( $$self{slurm_status}{$state} eq $$self{Error} )
    {
        $$jobs{Error}{$id} = $$jobs{Running}{$id};
        $$jobs{Error}{$id}{nfailures}++;
        delete($$jobs{Running}{$id});
        $dirty = 1;
    }
    else
    {
        $self->throw("This should not happen\n");
    }
    return $dirty;
}

sub job_running
{
    my ($self,$task) = @_;
    return $$task{status} & $$self{Running};
}

sub job_done
{
    my ($self, $task) = @_;
    return $$task{status} & $$self{Done};
}

sub job_failed
{
    my ($self, $task) = @_;
    return $$task{status} & $$self{Error};
}

sub job_nfailures
{
    my ($self, $task) = @_;
    return $$task{nfailures} ? $$task{nfailures} : 0;
}

sub set_limits
{
    my ($self,%limits) = @_;
    $$self{limits} = { %{$$self{limits}}, %limits };
}

sub clean_jobs
{
    my ($self,$job_name,$ids,$all_done) = @_;
    if ( $all_done ) { `rm -f $job_name.status.*`; return; }
    for my $id (@$ids) { `rm -f $job_name.status.$id.*`; }
}

sub kill_job
{
    my ($self,$job) = @_;
}

sub past_limits
{
    my ($self,$jid,$output) = @_; 
    return ();
}

sub _create_bsub_ids_string
{
    my ($self,$job_name,$ids) = @_;

    # Process the list of IDs. The maximum job name length is 255 characters. (For LSF, don't know about SLURM)
    my @ids = sort { $a<=>$b } @$ids;
    my @bsub_ids;
    my $from = $ids[0];
    my $prev = $from;
    for (my $i=1; $i<@ids; $i++)
    {
        my $id = $ids[$i];
        if ( $id != $prev+1 )
        {
            if ( $prev>$from ) { push @bsub_ids, "$from-$prev"; }
            else { push @bsub_ids, $from; }
            $from = $id;
            $prev = $id;
        }
        $prev = $id;
    }
    if ( $prev>$from ) { push @bsub_ids, "$from-$prev"; }
    else { push @bsub_ids, $from; }
    my $bsub_ids  = join(',', @bsub_ids);
    my @skipped_bsub_ids;
    while ( length($job_name) + length($bsub_ids) > 250 && scalar @bsub_ids ) 
    {
        push @skipped_bsub_ids, pop(@bsub_ids);
        $bsub_ids = join(',', @bsub_ids);
    }
    @$ids = ();
    foreach my $bsub_id (@skipped_bsub_ids)
    {
        if ($bsub_id =~ m/(\d+)-(\d+)/) { push @$ids, ($1..$2); }
        else { push @$ids, $bsub_id; }
    }
    return $bsub_ids;
}
sub _parse_bsub_ids_string
{
    my ($str) = @_;
    my @out = ();
    my @list = split(/,/,$str);
    for my $item (@list)
    {
        my ($from,$to) = split(/-/,$item);
        if ( !defined $to ) { $to = $from; }
        for (my $i=$from; $i<=$to; $i++) { push @out,$i; }
    }
    return \@out;
}
sub run_jobs
{
    my ($self,$job_name,$cmd,$ids) = @_;

    if ( !scalar @$ids ) { confess("No IDs given??\n"); }

    my $jobs = $self->_read_jobs($job_name);
    my $jids_file = "$job_name.jid";
    my $cmd_file  = "$job_name.sh";

    $cmd =~ s/{JOB_INDEX}/\$SLURM_ARRAY_TASK_ID/g;
    open(my $fh,'>',$cmd_file) or $self->throw("$cmd_file: $!");
    print $fh "#!/bin/sh\n";
    print $fh "touch $job_name.status.\$SLURM_ARRAY_TASK_ID.\$SLURM_ARRAY_JOB_ID.\$SLURM_JOBID\n";
    print $fh "$cmd\n";
    close($fh) or $self->throw("close failed: $cmd_file");
    chmod(0755,$cmd_file);

    my @ids = @$ids;
    while ( @ids )
    {
        my $bsub_ids = $self->_create_bsub_ids_string($job_name,\@ids);
        my $bsub_cmd = qq[sbatch --parsable --array='$bsub_ids' -e $job_name.rmme.\%a.e -o $job_name.\%a.o $cmd_file];

        # Submit to SLURM
        print STDERR "$bsub_cmd\n";
        my @out = `$bsub_cmd 2>&1`;
        if ( scalar @out!=1 || !($out[0]=~/^(\d+)$/) )
        {
            my $cwd = `pwd`;
            confess("Expected different output from sbatch. The command was:\n\t$cmd\nThe sbatch command was:\n\t$bsub_cmd\nThe working directory was:\n\t$cwd\nThe output was:\n", @out);
        }
        my $array_id = $1;

        my $list = _parse_bsub_ids_string($bsub_ids);
        for my $id (@$list)
        {
            $$jobs{Running}{$id} = { array_id=>$array_id, job_id=>-1, nfailures=>0 };
            if ( exists($$jobs{Error}{$id}) ) 
            { 
                $$jobs{Running}{$id}{nfailures} = $$jobs{Error}{$id}{nfailures}; 
                delete($$jobs{Error}{$id});
            }
            if ( exists($$jobs{Done}{$id}) ) { $self->throw("This should not happen: $id already done .. $job_name"); }
        }
    }
    $self->_write_jobs($job_name,$jobs);
}

# The jids file format is simple:
#   - id            .. runner's job id, same as SLURM_ARRAY_TASK_ID (%a) and `squeue -o '%K'`
#   - array id      .. -1: errored, 0: finished, int: SLURM_ARRAY_JOB_ID (%A), same as id returned by `sbatch` and `squeue -o '%F'`
#   - job id        .. -1: unknown, 0: errored/finished, int: SLURM_JOBID (%J), same as `squeue -o '%A'`
#   - nfailures     .. number of failures
sub _write_jobs
{
    my ($self,$job_name,$jobs) = @_;
    my $jids_file = "$job_name.jid";
    open(my $fh,'>',$jids_file) or $self->throw("$jids_file: $!\n");
    for my $id (keys %{$$jobs{Running}})
    {
        my $job = $$jobs{Running}{$id};
        print $fh "$id\t$$job{array_id}\t$$job{job_id}\t$$job{nfailures}\n";
    }
    for my $id (keys %{$$jobs{Error}})
    {
        my $job = $$jobs{Error}{$id};
        print $fh "$id\t-1\t0\t$$job{nfailures}\n";
    }
    for my $id (keys %{$$jobs{Done}})
    {
        my $job = $$jobs{Done}{$id};
        print $fh "$id\t0\t0\t$$job{nfailures}\n";
    }
    close($fh) or $self->throw("close failed: $jids_file\n");
}
sub _read_jobs
{
    my ($self,$job_name) = @_;

    my $jobs = { Running=>{}, Done=>{}, Error=>{} };

    my $jids_file = "$job_name.jid";
    if ( !-e $jids_file ) { return $jobs; }

    open(my $fh,'<',$jids_file) or $self->throw("$jids_file: $!");
    while (my $line=<$fh>)
    {
        my ($id,$array_id,$job_id,$nfailures) = split(/\t/,$line);
        chomp($nfailures);
        my $status = 'Running';
        if ( $array_id==-1 ) { $status = 'Error'; }
        elsif ( $array_id==0 ) { $status = 'Done'; }
        $$jobs{$status}{$id} = { array_id=>$array_id, job_id=>$job_id, nfailures=>$nfailures };
    }
    close($fh) or $self->throw("close failed: $jids_file\n");
    return $jobs;
}

sub reset_step
{
    my ($self,$job_name) = @_;
    `rm -f $job_name.jid $job_name.status.*`;
}

=head1 AUTHORS

petr.danecek@sanger

=head1 COPYRIGHT AND LICENSE

The MIT License

Copyright (C) 2012-2015 Genome Research Ltd.

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in
all copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
THE SOFTWARE.

=cut

1;


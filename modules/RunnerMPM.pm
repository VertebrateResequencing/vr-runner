=head1 NAME

RunnerMPM.pm   - Runner pipeline framework for a single multi-processor machine

=head1 SYNOPSIS

All Runner pipelines can be run without modification on a single multi-processor
machine. Just add "+js mpm" or "+js MPM" to your runner command. The documentation
below described the necessary minimum to extend this framework to other batch job
schedulers.

=head1 METHODS

=cut

package RunnerMPM;

use strict;
use warnings;
use Carp;
use Runner;
use IPC::Run3;

sub new
{
    my ($class,@args) = @_;
    my $self = @args ? {@args} : {};
    bless $self, ref($class) || $class;

    $$self{Running} = 1;
    $$self{Error}   = 2;
    $$self{Done}    = 4;

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

=head2 init_jobs

    About : Initialize jobs given a list of numeric ids. This may be a relatively 
            expensive operation which determines the status for a whole batch
            of jobs at once. The main Runner.pm module does not know about the
            content of these objects, they can be arbitrary structures (usually
            hashes) which will be passed back to the job scheduler via the
            following functions that the scheduler must implement:
                job_running
                job_done
                job_failed
                job_nfailures
                kill_job
                past_limits
                run_jobs
    Usage : $self->get_jobs($job_name,$ids);
    Args  :
            $job_name <string> 
                The job name serves as a key for a quick retrieval of jobs which
                are running, pending, or completed in this step. It is safe to assume
                that the job name is a unique file location in a mounted file system.
                The runners implemented so far store all information in small text files 
                but nothing prevents the job scheduler from using a database.
            $ids <array of ints>
                Array reference with a list of numeric ids. Combined with $jobs_name,
                these form a unique identifier for each spawn()-ed job.

=cut

sub init_jobs
{
    my ($self, $job_name, $ids) = @_;

    my $jobs = $self->_read_jobs($job_name);
    my @jobs_out = ();
    for my $id (@$ids)
    {
        my $status = 0;
        my $nfails = 0;
        if ( exists($$jobs{Running}{$id}) ) { $status = $$self{Running}; }
        elsif ( exists($$jobs{Done}{$id}) ) { $status = $$self{Done}; }
        elsif ( exists($$jobs{Error}{$id}) ) { $status = $$self{Error}; $nfails = $$jobs{Error}{$id}{nfailures}; }
        push @jobs_out, {status=>$status, nfailures=>$nfails};
    }
    return \@jobs_out;
}

=head2 job_running

    About : Check if a job is still running
    Usage : $self->job_running($job);
    Args  : An object returned by get_jobs

=cut

sub job_running
{
    my ($self,$task) = @_;
    return $$task{status} & $$self{Running};
}

=head2 job_done

    About : Check if a job has finished. This is a supplementary method to
            Runner's is_finished method, can be empty.
    Usage : $self->job_done($job);
    Args  : An object returned by get_jobs

=cut

sub job_done
{
    my ($self, $task) = @_;
    return $$task{status} & $$self{Done};
}

=head2 job_failed

    About : Check if a job has failed
    Usage : $self->job_failed($job);
    Args  : An object returned by get_jobs

=cut

sub job_failed
{
    my ($self, $task) = @_;
    return $$task{status} & $$self{Error};
}

=head2 job_nfailures

    About : How many times has the job failed?
    Usage : $self->job_nfailures($job);
    Args  : An object returned by get_jobs

=cut

sub job_nfailures
{
    my ($self, $task) = @_;
    return $$task{nfailures} ? $$task{nfailures} : 0;
}

=head2 set_limits

    About : Limit the resources
    Usage : $self->set_limits(max_jobs=>3);
    Args  : See the documentation of the same function in Runner.pm

=cut

sub set_limits
{
    my ($self,%limits) = @_;
    $$self{limits} = { %{$$self{limits}}, %limits };
}

=head2 past_limits

    About : Return a hash with the maximum required limits for this job so far.
            This is necessary for increasing limits of a failing job. Currently
            only MEMLIMIT is checked for by Runner.pm.
    Usage : $self->past_limits($job);
    Args  : An object returned by get_jobs

=cut

sub past_limits
{
    my ($self,$task) = @_; 
    return ();
}

=head2 clean_jobs

    About : Clean temporary data after a job has finished. This has been
            in use with LSFCR only so far.
    Usage : $self->clean_jobs($job_name,$ids,$all_done);
    Args  : See the usage in RunnerLSFCR

=cut

sub clean_jobs
{
    my ($self,$job_name,$ids,$all_done) = @_;
}

=head2 kill_job

    About : Kill the job
    Usage : $self->kill_job($job);
    Args  : An object returned by get_jobs

=cut

sub kill_job
{
    my ($self,$job) = @_;
}

=head2 run_jobs

    About : Submit the command for each id. The command line contains the string
            {JOB_INDEX} which must be replaced with one or more of the supplied
            ids, the exact form depends on the job scheduler.
    Usage : $self->run_jobs($job_name,$cmd,$ids);
    Args  : 
            $job_name <string> 
                The job name
            $cmd <string> 
                The command line to run, with the substring {JOB_INDEX} to be
                expanded as required by the job scheduler
            $ids <array of ints>
                Array reference with a list of numeric ids

=cut

sub run_jobs
{
    my ($self,$job_name,$cmd,$ids) = @_;

    my $jobs = $self->_read_jobs($job_name);

    # Running at full capacity
    if ( scalar keys %{$$jobs{Running}} >= $$self{limits}{max_jobs} ) { return; }

    my $dirty = 0;
    for my $id (@$ids)
    {
        if ( exists($$jobs{Running}{$id}) ) { next; }
        if ( exists($$jobs{Done}{$id}) ) { next; }

        my $cmd1 = $cmd;
        $cmd1    =~ s/{JOB_INDEX}/$id/g;

        my $pid = fork;
        if ( !defined $pid ) { $self->throw("Fork failed\n"); }
        if ( $pid==0 )
        {
            # child process
            open(my $stderr,'>>',"$job_name.$id.e") or confess("$job_name.$id.e: $!");
            open(my $stdout,'>>',"$job_name.$id.o") or confess("$job_name.$id.o: $!");

            my @cmd = ('/bin/bash', '-o','pipefail','-c', $cmd1);
            run3 \@cmd,undef,$stdout,$stderr;

            # this tells the parent if job finished ok or errored
            my $fname = "$job_name.$id.status";
            open(my $fh,'>',"$fname.part") or $self->throw("$fname.part: $!");
            print $fh ($? ? 'err' : 'ok'),"\n";
            close($fh) or $self->throw("close failed: $fname.part");
            rename("$fname.part",$fname);
            exit $?>>8;
        }
        # parent process

        $dirty = 1;
        $$jobs{Running}{$id} = { pid=>$pid, nfailures=>0, cmd=>$cmd1 };
        if ( exists($$jobs{Error}{$id}) ) 
        { 
            $$jobs{Running}{$id}{nfailures} = $$jobs{Error}{$id}{nfailures}; 
            delete($$jobs{Error}{$id});
        }
        if ( scalar keys %{$$jobs{Running}} >= $$self{limits}{max_jobs} ) { last; }
    }
    if ( $dirty ) 
    { 
        $self->_write_jobs($job_name,$jobs); 
    }
}

# The jids file format is simple:
#   - array index   .. runner's job id
#   - status        .. -1: errored, 0: finished, <int>: running PID
#   - nfailures     .. number of failures
#   - command       .. bsub command
sub _write_jobs
{
    my ($self,$job_name,$jobs) = @_;
    my $jids_file = "$job_name.jid";
    open(my $fh,'>',$jids_file) or $self->throw("$jids_file: $!\n");
    for my $id (keys %{$$jobs{Running}})
    {
        my $job = $$jobs{Running}{$id};
        print $fh "$id\t$$job{pid}\t$$job{nfailures}\t$$job{cmd}\n";
    }
    for my $id (keys %{$$jobs{Error}})
    {
        my $job = $$jobs{Error}{$id};
        print $fh "$id\t-1\t$$job{nfailures}\t$$job{cmd}\n";
    }
    for my $id (keys %{$$jobs{Done}})
    {
        my $job = $$jobs{Done}{$id};
        print $fh "$id\t0\t$$job{nfailures}\t$$job{cmd}\n";
    }
    close($fh) or $self->throw("close failed: $jids_file\n");
}
sub _read_jobs
{
    my ($self,$job_name) = @_;

    my $jobs = { Running=>{}, Done=>{}, Error=>{} };

    my $jids_file = "$job_name.jid";
    if ( !-e $jids_file ) { return $jobs; }

    # get a list of all running jobs
    my @running = `ps --no-headers -o pid,command -e`;
    my %running = ();
    for my $cmd (@running)
    {
        if ( !($cmd=~/^\s*(\d+)\s+(.*)\s*$/) ) { next; }
        $running{$1} = $2;
    }
    my $script_name = $0;
    $script_name =~ s{^.*/}{};

    open(my $fh,'<',$jids_file) or $self->throw("$jids_file: $!");
    while (my $line=<$fh>)
    {
        my ($id,$pid,$nfailures,@cmd) = split(/\t/,$line);
        chomp($cmd[-1]);
        my $status;
        if ( $pid>0 ) 
        { 
            # The process was running last time we checked
            $status = 'Running';
            if ( !$running{$pid} or !($running{$pid}=~/$script_name/) )
            {
                # unknown state, treat as if the process was not running
                if ( !-e "$job_name.$id.status" ) { next; }
                open(my $fh,'<',"$job_name.$id.status") or $self->throw("$job_name.$id.status: $!");
                my @status = <$fh>;
                if ( $status[0] =~ /^err$/ ) { $status = 'Error'; $nfailures++; }
                elsif ( $status[0] =~ /^ok$/ ) { $status = 'Done'; $nfailures = 0; }
                close($fh) or $self->throw("close failed: $job_name.$id.status");
            }
        }
        elsif ( $pid==0 ) { $status = 'Done'; }
        else { $status = 'Error'; }
        $$jobs{$status}{$id} = { pid=>$pid, nfailures=>$nfailures, cmd=>join("\t",@cmd) };
    }
    close($fh) or $self->throw("close failed: $jids_file\n");
    return $jobs;
}

=head2 reset_step

    About : Reset status of the step so that failing jobs are forgotten and +retries
            is not required anymore
    Usage : $self->reset_setp($job_name);
    Args  : Step id

=cut

sub reset_step
{
    my ($self,$job_name) = @_;
    my $jids_file = "$job_name.jid";
    unlink($jids_file);
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


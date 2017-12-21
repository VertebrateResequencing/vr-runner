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

    #$$self{verbose} = 2;

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
        SPECIAL_EXIT => $$self{Error},
        COMPLETED    => $$self{Done},
    };

    $$self{default_limits} = { memory=>500, runtime=>60 };
    $$self{limits} = { %{$$self{default_limits}} };

    return $self;
}

sub throw
{
    my ($self,@msg) = @_;
    confess(@msg);
}

sub init_jobs
{
    my ($self, $job_name, $ids) = @_;
    my $jids_file = "$job_name.jid";

    # For each input job id create a hash with info: status, number of failuers
    if ( ! -e $jids_file ) 
    { 
        my @jobs_out = ();
        for my $id (@$ids) { push @jobs_out, {status=>$$self{Unknown}, nfailures=>0, memlimit=>0, runtime=>0}; }
        return \@jobs_out; 
    }

    my $jobs  = $self->_read_jobs($job_name);
    my $dirty = 0;
    my @jobs_out = ();
    for my $id (@$ids)
    {
        my $job = { status=>$$self{Unknown} };
        if ( exists($$jobs{Running}{$id}) )
        {
            $dirty += $self->_update_job_status($job_name,$jobs,$id);
        }
        if ( exists($$jobs{Running}{$id}) ) { $job = { %{$$jobs{Running}{$id}}, status=>$$self{Running} }; }
        elsif ( exists($$jobs{Done}{$id}) ) { $job = { %{$$jobs{Done}{$id}}, status=>$$self{Done} }; }
        elsif ( exists($$jobs{Error}{$id}) ) { $job = { %{$$jobs{Error}{$id}}, status=>$$self{Error} }; }
        push @jobs_out, $job;
    }
    delete($$self{cached_slurm_jobs});
    if ( $dirty ) { $self->_write_jobs($job_name,$jobs); }
    return \@jobs_out;
}

sub _parse_mem
{
    my ($self,$mem) = @_;
    if ( !defined $mem or $mem eq '' ) { return 0; }
    if ( $mem=~/^(\d+\.?\d*)(\D)/ )
    {
        if ( lc($2) eq 'm' ) { $mem = $1; }
        elsif ( lc($2) eq 'k' ) { $mem = int($1/1e3); }
        elsif ( lc($2) eq 'g' ) { $mem = int($1*1e3); }
        else { $self->throw("todo mem: $mem\n"); }
    }
    return $mem;
}
sub _parse_elapsed
{
    my ($self,$elapsed) = @_;

    # min
    # min:sec
    # hour:min:sec
    # day-hour
    # day-hour:min
    # day-hour:min:sec

    my @vals = split(/:/,$elapsed);
    if ( @vals==3 or (@vals==2 && !($vals[0]=~/-/)) ) { pop @vals; }  # ignore seconds

    # min
    # hour:min
    # day-hour
    # day-hour:min
    if ( $vals[0]=~/-/ )
    {
        my ($day,$hour) = split(/-/,$vals[0]); 
        $vals[0] = $day*24 + $hour;
    }

    # min
    # hour:min
    if ( @vals==2 ) { $vals[1] += 60*$vals[0]; shift(@vals); }

    return $vals[0];
}

# Parse the output of `sacct -n -P -o alloctres,elapsed`, already split in fields
sub _parse_slurm_limits
{
    my ($self,$alloctres,$elapsed) = @_;
    my $mem = !($alloctres=~/mem=(\d+\D)/) ? 0 : $self->_parse_mem($1);
    my $cpu = $self->_parse_elapsed($elapsed);
    return ( memlimit=>$mem, runtime=>$cpu );
}
# Parse the output of `sacct -n -P -o state
sub _parse_slurm_status
{
    my ($self,$status) = @_;
    if ( !exists($$self{slurm_status}{$status}) ) { $self->throw("SLURM status todo: $status\n"); }
    return $$self{slurm_status}{$status};
}
# Parse the output of `sacct -n -P -o jobid
sub _parse_slurm_jobid
{
    my ($self,$jobid) = @_;
    if ( $jobid =~ /\.batch$/ or $jobid =~ /\.extern$/ ) { return []; }
    if ( $jobid =~ /^\d+_\d+$/ or $jobid =~ /^\d+$/ ) { return [ $jobid ]; }
    if ( $jobid =~ /^(\d+)_\[/ )
    {
        my $arrid = $1;
        my $ids = $';
        $ids =~ s/\]//;
        $ids = _parse_bsub_ids_string($ids);
        my @ids = ();
        for my $id (@$ids) { push @ids,"${arrid}_$id"; }
        return \@ids;
    }
    return [];
}

# find out the job's status and slurm job id - only the array index
# (runner's id) and slurm array id is known
sub _update_job_status
{
    my ($self,$job_name,$jobs,$id) = @_;

    my $job = $$jobs{Running}{$id};

    if ( !exists($$job{array_id}) ) { $self->throw("No job array id for $job_name.$id?\n"); }
    my $array_id = $$job{array_id};
    my $slurm_id = $array_id.'_'.$id;

    # This is a short-lived cache. If it exists, we just learned the status of
    # this array in the previous batch of _update_job_status() calls.
    if ( !exists($$self{cached_slurm_jobs}) )
    {
        my $cmd = qq[sacct -n -P -o state,alloctres,elapsed,jobid -u $ENV{USER}];
        if ( $$self{verbose} > 1 ) { print STDERR "$cmd\n"; }
        my @lines = `$cmd`;
        if ( $? ) { $self->throw("The command returned non-zero status `$cmd`: $?, $!"); }
        for my $line (@lines)
        {
            my ($status,$alloctres,$elapsed,$jobid) = split(/\|/,$line);
            chomp($jobid);
            my %limits = $self->_parse_slurm_limits($alloctres, $elapsed);
            my $ids = $self->_parse_slurm_jobid($jobid);
            for my $id (@$ids)
            {
                $$self{cached_slurm_jobs}{$id}{status} = $self->_parse_slurm_status($status);
                $$self{cached_slurm_jobs}{$id}{limits} = { %limits };
            }
        }
    }

    if ( !exists($$self{cached_slurm_jobs}{$slurm_id}) )
    {
        if ( exists($$self{cached_slurm_jobs}{$array_id}) ) { return 0; }   # pending
        return 0;   # Assume it is not in the queue yet. Check me: this can be a problem
    }

    if ( $$self{cached_slurm_jobs}{$slurm_id}{status} == $$self{Running} ) { return 0; }    
    if ( $$self{cached_slurm_jobs}{$slurm_id}{status} == $$self{Done} )
    {
        $$jobs{Done}{$id} = $$jobs{Running}{$id};
        delete($$jobs{Running}{$id});
        return 1;   # dirty
    }    
    if ( $$self{cached_slurm_jobs}{$slurm_id}{status} != $$self{Error} ) { $self->throw("todo: $slurm_id .. $$self{cached_slurm_jobs}{$slurm_id}{status}\n"); }

    my $memlimit = $$self{cached_slurm_jobs}{$slurm_id}{limits}{memlimit};
    my $runtime  = $$self{cached_slurm_jobs}{$slurm_id}{limits}{runtime};

    # Unfortunately, SLURM does not reliably report exceeded memory. Here
    # we rely on the following error message in the error output file:
    #   "slurmstepd: Exceeded step memory limit at some point. Step may have been partially swapped out to disk."
    #
    if ( open(my $fh,'<',"$job_name.$id.e") )
    {
        # slurmstepd: Exceeded step memory limit at some point. Step may have been partially swapped out to disk.
        # slurmstepd: *** JOB 915517 CANCELLED AT 2016-01-12T21:52:10 DUE TO TIME LIMIT ***
        while (my $line=<$fh>)
        {
            if ( $line=~/^slurmstepd:.+Exceeded .+memory limit/ ) { $memlimit = -abs($memlimit); }
            if ( $line=~/^slurmstepd:/ && $line=~/DUE TO TIME LIMIT/i ) { $runtime = -abs($runtime); }
        }
        close($fh);
    }
    if ( $memlimit < 0 ) { $memlimit *= 1.1; }

    $$jobs{Error}{$id} = $$jobs{Running}{$id};
    if ( abs($$jobs{Error}{$id}{memlimit}) < abs($memlimit) ) { $$jobs{Error}{$id}{memlimit} = $memlimit; }
    if ( abs($$jobs{Error}{$id}{runtime}) < abs($runtime) ) { $$jobs{Error}{$id}{runtime} = $runtime; }
    $$jobs{Error}{$id}{nfailures}++;
    delete($$jobs{Running}{$id});

    return 1;    # dirty
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
}

sub kill_job
{
    my ($self,$task) = @_;
}

sub past_limits
{
    my ($self,$task) = @_; 
    my %out = ();
    if ( exists($$task{memlimit}) )
    {
        $out{memory} = abs($$task{memlimit});
        if ( $$task{memlimit}<0 ) { $out{MEMLIMIT} = abs($$task{memlimit}); }
    }
    if ( exists($$task{runtime}) )
    {
        $out{runtime} = abs($$task{runtime});
        if ( $$task{runtime}<0 ) { $out{TIMELIMIT} = abs($$task{runtime}); }
    }
    return %out;
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
sub _bsub_command
{
    my ($self,$bsub_cmd,$cmd) = @_;
    for (my $i=2; $i<15; $i++)
    {
        if ( $$self{verbose} > 1 ) { print STDERR "$bsub_cmd\n"; }
        my @out = `$bsub_cmd 2>&1`;
        if ( $? )
        {
            if ( $out[0] =~ /Slurm temporarily unable to accept job, sleeping and retrying/ )
            {
                print STDERR "lsadmin failed, trying again in $i sec...\n";
                sleep $i; 
                next; 
            }
            my $cwd = `pwd`;
            confess("Expected different output from sbatch. The command was:\n\t$cmd\nThe sbatch command was:\n\t$bsub_cmd\nThe working directory was:\n\t$cwd\nThe output was:\n", @out);
        }
        if ( scalar @out!=1 || !($out[0]=~/^(\d+)$/) )
        {
            my $cwd = `pwd`;
            confess("Expected different output from sbatch. The command was:\n\t$cmd\nThe sbatch command was:\n\t$bsub_cmd\nThe working directory was:\n\t$cwd\nThe output was:\n", @out);
        }
        return $1;
    }
    confess("The sbatch command failed repeatedly: $bsub_cmd");
}
sub run_jobs
{
    my ($self,$job_name,$cmd,$ids) = @_;

    if ( !scalar @$ids ) { confess("No IDs given??\n"); }

    my $jobs = $self->_read_jobs($job_name);
    my $jids_file = "$job_name.jid";
    my $cmd_file  = "$job_name.sh";

    $cmd =~ s/{JOB_INDEX}/\$SLURM_ARRAY_TASK_ID/g;

    my $runtime = 0;
    for my $id (@$ids)
    {
        if ( exists($$jobs{all}{$id}) && $runtime < abs($$jobs{all}{$id}{runtime}) )  
        { 
            $runtime = abs($$jobs{all}{$id}{runtime}); 
            if ( $$jobs{all}{$id}{runtime} < 0 ) { $runtime *= 2; }
        }
    }

    my @ids = @$ids;
    while ( @ids )
    {
        my $bsub_ids = $self->_create_bsub_ids_string($job_name,\@ids);
        my $used_ids = _parse_bsub_ids_string($bsub_ids);

        my $mem = 0;
        if ( $$self{limits}{memory} ) { $mem = int($$self{limits}{memory}); }
        elsif ( $$self{default_limits}{memory} ) { $mem = int($$self{defaults_limits}{memory}); }
        my $memory = $mem ? "--mem-per-cpu=$mem" : '';

        # my $mem_per_node = '';
        # if ( $$self{limits}{memory_per_node} ) { $mem_per_node = '--mem='.int($$self{limits}{memory_per_node}); }
        # elsif ( $$self{default_limits}{memory_per_node} ) { $mem_per_node = '--mem='.int($$self{defaults_limits}{memory_per_node}); }

        if ( $runtime < $$self{limits}{runtime} ) { $runtime = $$self{limits}{runtime}; }
        if ( $runtime < $$self{default_limits}{runtime} ) { $runtime = $$self{default_limits}{runtime}; }
        $runtime = int($runtime);
        my $run_time = $runtime ? "--time=$runtime" : '';

        my $ncpu = exists($$self{limits}{cpus}) ? $$self{limits}{cpus} : 1;
        $ncpu = "--cpus-per-task=$ncpu";

        my $bsub_cmd = qq[sbatch --parsable $run_time $memory $ncpu --array='$bsub_ids' -e $job_name.\%a.e -o $job_name.\%a.o --wrap='$cmd'];

        # Submit to SLURM
        my $array_id = $self->_bsub_command($bsub_cmd,$cmd);

        for my $id (@$used_ids)
        {
            # For debugging
            if ( $$self{verbose} > 1 )
            {
                if ( -e "$job_name.$id.e" )
                { 
                    `cat $job_name.$id.e >> $job_name.$id.e.prev`;
                }
                open(my $fh,'>>',"$job_name.$id.e.prev") or $self->throw("$job_name.$id.e.prev: $!");
                print $fh "\n\n$bsub_cmd\n";
                close($fh) or $self->throw("close failed: $job_name.$id.e.prev");
            }

            $$jobs{Running}{$id} = { array_id=>$array_id, nfailures=>0, memlimit=>$mem, runtime=>$runtime };
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
#   - nfailures     .. number of failures
#   - memlimit      .. max memory used, negative if memory limit was exceeded, 0 for unknown [MB]
#   - runtime       .. running time, negative if time limit was exceeded, 0 for unknown [minutes]
sub _write_jobs
{
    my ($self,$job_name,$jobs) = @_;
    my @unlink = ();
    my $jids_file = "$job_name.jid";
    open(my $fh,'>',$jids_file) or $self->throw("$jids_file: $!\n");
    for my $id (keys %{$$jobs{Running}})
    {
        my $job = $$jobs{Running}{$id};
        print $fh "$id\t$$job{array_id}\t$$job{nfailures}\t$$job{memlimit}\t$$job{runtime}\n";
        if ( exists($$job{unlink}) ) { push @unlink,$$job{unlink}; }
    }
    for my $id (keys %{$$jobs{Error}})
    {
        my $job = $$jobs{Error}{$id};
        print $fh "$id\t-1\t$$job{nfailures}\t$$job{memlimit}\t$$job{runtime}\n";
        if ( exists($$job{unlink}) ) { push @unlink,$$job{unlink}; }
    }
    for my $id (keys %{$$jobs{Done}})
    {
        my $job = $$jobs{Done}{$id};
        print $fh "$id\t0\t$$job{nfailures}\t$$job{memlimit}\t$$job{runtime}\n";
        if ( exists($$job{unlink}) ) { push @unlink,$$job{unlink}; }
    }
    close($fh) or $self->throw("close failed: $jids_file\n");
    # print STDERR "jobs written: $job_name.jid\n"; print `cat $job_name.jid`;
    for my $file (@unlink) 
    {
        # print STDERR "unlink $file\n";
        unlink($file);
    }
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
        chomp($line);
        my ($id,$array_id,$nfailures,$memlimit,$runtime) = split(/\t/,$line);
        my $status = 'Running';
        if ( $array_id==-1 ) { $status = 'Error'; }
        elsif ( $array_id==0 ) { $status = 'Done'; }
        $$jobs{$status}{$id} = { id=>$id, array_id=>$array_id, nfailures=>$nfailures, memlimit=>$memlimit, runtime=>$runtime };
        $$jobs{all}{$id} = { id=>$id, array_id=>$array_id, nfailures=>$nfailures, memlimit=>$memlimit, runtime=>$runtime };
    }
    close($fh) or $self->throw("close failed: $jids_file\n");
    # print STDERR "jobs read: $job_name.jid\n"; print `cat $job_name.jid`;
    return $jobs;
}

sub reset_step
{
    my ($self,$job_name) = @_;
    `rm -f $job_name.jid`;
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


package RunnerLSF;

use strict;
use warnings;
use Carp;
use POSIX;

sub new
{
    my ($class,@args) = @_;
    my $self = @args ? {@args} : {};
    bless $self, ref($class) || $class;

    $$self{Running} = 1;
    $$self{Error}   = 2;
    $$self{Zombi}   = 4;
    $$self{No}      = 8;
    $$self{Done}    = 16;
    $$self{Waiting} = 32;

    $$self{lsf_status_codes} =
    {
        DONE  => $$self{Done},
        PEND  => $$self{Running} | $$self{Waiting},
        WAIT  => $$self{Running} | $$self{Waiting},
        EXIT  => $$self{Error},
        ZOMBI => $$self{Zombi},
        RUN   => $$self{Running},
        UNKWN => $$self{Running},
        SSUSP => $$self{Running},
        USUSP => $$self{Running}
    };

    # runtime and queue_limits are in minutes
    $$self{default_limits} = { runtime=>40, memory=>1_000, queue=>'normal' };
    $$self{queue_limits}   = { basement=>1e9, week=>7*24*60, long=>48*60, normal=>12*60, small=>30 };

    # Can be one of: kB,MB,GB or "ask" for `lsadmin showconf lim`
    if ( !exists($$self{lsf_limits_unit}) ) { $$self{lsf_limits_unit} = 'MB'; }

    $self->_set_lsf_limits_unit();
    $self->_init_zombies();

    return $self;
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
# runtime and queue_limits are in minutes
sub set_limits
{
    my ($self,%limits) = @_;
    $$self{limits} = { %{$$self{default_limits}}, %limits };
    if ( exists($limits{queues}) )
    {
        $$self{queue_limits} = { %{$limits{queues}} };
    }
}
sub clean_jobs
{
    my ($self,$wfile,$ids) = @_;
}
sub kill_job
{
    my ($self,$job) = @_;
    if ( !exists($$job{lsf_id}) ) { return; }
    my $cmd = "bkill -s KILL -b '$$job{lsf_id}'";
    warn("$cmd\n");
    `$cmd`;
}

sub _init_zombies
{
    my ($self) = @_;
    if ( !exists($ENV{LSF_ZOMBI_IS_DEAD}) ) { return; }
    my @arrays = split(/\./,$ENV{LSF_ZOMBI_IS_DEAD});
    for my $arr (@arrays)
    {
        if ( !($arr=~/^(\d+)\[(.+)\]$/) ) { confess("Could not parse LSF_ZOMBI_IS_DEAD=$ENV{LSF_ZOMBI_IS_DEAD}\n"); }
        my $id  = $1;
        my $ids = $2;
        my @items = split(/,/,$ids);
        for my $item (@items)
        {
            if ( $item=~/^(\d+)$/ ) { $$self{ignore_zombies}{"${id}[$1]"} = 1; next; }
            my ($from,$to) = split(/-/,$item);
            for (my $i=$from; $i<=$to; $i++)
            {
                $$self{ignore_zombies}{"${id}[$i]"} = 1;
            }
        }
    }
}

sub init_jobs
{
    my ($self, $job_name, $ids) = @_;
    my $jids_file = "$job_name.jid";

    # For each input job id create a hash with info: status, number of failuers
    my @jobs_out = ();
    for my $id (@$ids) { push @jobs_out, {status=>$$self{No}, nfailures=>0}; }
    if ( ! -e $jids_file ) { return \@jobs_out; }

    # The same file with job ids may contain many job arrays runs. Check
    #   the jobs in the reverse order in case there were many failures.
    #   The last success counts, failures may be discarded in such a case.
    #
    open(my $fh, '<', $jids_file) or confess("$jids_file: $!");
    my $path = $job_name; $path =~ s{/+}{/}g; $path =~ s{^\./}{};  # ignore multiple dir separators (dir//file vs dir/file)
    my @jids = ();
    my @jid_lines = ();
    while (my $line=<$fh>)
    {
        push @jid_lines,$line;
        if ( !($line=~/^(\d+)\s+([^\t]*)/) ) { confess("Uh, could not parse \"$line\".\n") }
        push @jids, $1;     # LSF array ID
        my $tmp = $2;
        $tmp =~ s{/+}{/}g;  # ignore multiple dir separators (dir//file vs dir/file)
        $tmp =~ s{^\./}{};
        if ( $path ne $tmp ) { confess("$path ne $tmp\n"); }
    }
    close($fh) or confess("close failed: $jids_file");

    # ZOMBI jobs need special care, we cannot be 100% sure that the non-responsive
    # node stopped writing to network disks. Let the user decide if they can be
    # safely ignored.
    my %zombi_warning = ();

    # The jids file may contain records from very old LSF runs. For each of them `bjobs -l`
    # is executed in _parse_bjobs_l which is expensive: record the info and clean the jid
    # file afterwards.
    my %no_bjobs_info = ();

    my $bjobs_info = $self->_parse_bjobs_l();

    # Get info from bjobs -l: iterate over LSF array IDs we remember for this task in the jids_file
    for (my $i=@jids-1; $i>=0; $i--)
    {
        if ( !exists($$bjobs_info{$jids[$i]}) )
        {
            $no_bjobs_info{$i} = $jids[$i];   # this LSF job is not listed by bjobs
            next;
        }
        my $info = $$bjobs_info{$jids[$i]};

        # Check if the time limits are still OK for all running jobs. Switch queues if not
        for my $job_l (values %$info)
        {
            if ( $$job_l{status} eq 'ZOMBI' && !$$self{ignore_zombies}{$$job_l{lsf_id}} ) { $zombi_warning{$$job_l{lsf_id}} = 1; }
            $self->_check_job($job_l,$jids_file);
        }

        # Update status of input jobs present in the bjobs -l listing. Note that the failed jobs
        # get their status from the output files as otherwise we wouldn't know how many times
        # they failed already.
        for (my $j=0; $j<@$ids; $j++)
        {
            my $id = $$ids[$j];
            if ( !exists($$info{$id}) ) { next; }
            if ( $jobs_out[$j]{status} ne $$self{No} ) { next; }   # the job was submitted multiple times and already has a status

            if ( $$info{$id}{status} & $$self{Done} )
            {
                $jobs_out[$j]{status} = $$self{Done};
            }
            elsif ( $$info{$id}{status} & $$self{Running} )
            {
                $jobs_out[$j]{status} = $$self{Running};
                $jobs_out[$j]{lsf_id} = $$info{$id}{lsf_id};
            }
            elsif ( $$info{$id}{status} & $$self{Zombi} )
            {
                # Set as failed if ZOMBI should be ignored, otherwise say it's running.
                my $lsf_id = $$info{$id}{lsf_id};
                if ( $$self{ignore_zombies}{$lsf_id} ) { $jobs_out[$j]{status} = $$self{Error}; }
                else { $jobs_out[$j]{status} = $$self{Running}; }
                $jobs_out[$j]{lsf_id} = $lsf_id;
            }
        }
    }

    # Update the jids file, removing old LSF jobs not known to bjobs anymore.
    # Hopefully this is safe: jobs is either pending (then must be present in
    # the listing) or gone.
    if ( scalar keys %no_bjobs_info )
    {
        open(my $fh, '>', "$jids_file.part") or confess("$jids_file.part: $!");
        for (my $i=0; $i<@jid_lines; $i++)
        {
            if ( exists($no_bjobs_info{$i}) ) { next; }
            print $fh $jid_lines[$i];
        }
        close($fh) or confess("close failed: $jids_file.part");
        rename("$jids_file.part",$jids_file) or confess("rename $jids_file.part $jids_file: $!");
    }

    if ( scalar keys %zombi_warning )
    {
        my %arrays = ();
        for my $lsf_id (keys %zombi_warning)
        {
            if ( $lsf_id =~ s/^(\d+)\[(\d+)\]$// ) { push @{$arrays{$1}},$2; }
        }
        my @id_strings = ();
        for my $id (keys %arrays)
        {
            while ( @{$arrays{$id}} )
            {
                push @id_strings, "${id}[". $self->_create_bsub_ids_string($id,$arrays{$id}) . "]";
            }
        }
        warn(
            "\n\n----\n\n" .
            "WARNING:  Some jobs were found in ZOMBI state and are still considered as\n" .
            "  running by the pipeline. To ignore these jobs, set the environment variable\n" .
            "       LSF_ZOMBI_IS_DEAD='" .join('.',@id_strings). "'\n" .
            "  and restart the pipeline. See also \"$jids_file\".\n" .
            "----\n\n"
            );
    }

    # For jobs which are not present in the bjobs -l listing we get the info from output files
    my $ntodo = 0;
    for (my $i=0; $i<@$ids; $i++)
    {
        if ( $jobs_out[$i]{status} & $$self{Running} || $jobs_out[$i]{status} & $$self{Error} ) { $ntodo++; }
        if ( $$self{limits}{max_jobs} && $ntodo >= $$self{limits}{max_jobs} ) { last; }
        if ( $jobs_out[$i]{status} ne $$self{No} ) { next; }
        my $info = $self->_parse_output($$ids[$i], $path);
        if ( defined $info )
        {
            $jobs_out[$i] = { %{$jobs_out[$i]}, %$info };
        }
    }
    return \@jobs_out;
}

sub date_to_time
{
    my (%t) = @_;
    # mktime arguments: sec, min, hour, mday, mon, year, wday = 0, yday = 0, isdst = -1
    return POSIX::mktime(0,$t{minute},$t{hour},$t{day},$t{month}-1,$t{year}-1900);


    #my $x = POSIX::mktime(0,$t{minute},$t{hour},$t{day},$t{month}-1,$t{year}-1900);
    #my $y = POSIX::ctime($x);
    #print STDERR "$t{minute} $t{hour} $t{day} $t{month} $t{year}\n";
    #print STDERR "$y\n";
    #exit;
}

sub _parse_bjobs_l
{
    my ($self,$debug_lsf_output) = @_;

    my @lines;
    if ( !defined $debug_lsf_output )
    {
        for (my $i=0; $i<3; $i++)
        {
            @lines = `bjobs -l 2>/dev/null`;
            if ( $? ) { sleep 5; next; }
            if ( !scalar @lines ) { return undef; }
        }
    }
    else
    {
        # If LSF output could not be parsed, debug by running
        #
        #   perl -e 'use RunnerLSF; $x=RunnerLSF->new; $x->_parse_bjobs_l(qq[debug-lsf-output.txt])'
        #
        @lines = `cat $debug_lsf_output`;
    }

    my %months = qw(Jan 1 Feb 2 Mar 3 Apr 4 May 5 Jun 6 Jul 7 Aug 8 Sep 9 Oct 10 Nov 11 Dec 12);
    my $year = (localtime())[5] + 1900;

    my $info = {};
    my $job;
    for (my $i=0; $i<@lines; $i++)
    {
        if ( $lines[$i]=~/^\s*$/ ) { next; }

        if ( $lines[$i]=~/^Job <(\d+)(.*)$/ )
        {
            # The job id can be 6866118[2759] or 6866118
            my $lsf_id = $1;    # the "6866118" part now or the whole lsf id "6866118[2759]" soon
            my $job_id = $1;    # the "2759" part soon
            my $arr_id = $1;    # the "6866118" part
            my $rest   = $2;    # the "[2759]" part
            if ( $rest=~/^\[(\d+)/ )
            {
                $lsf_id = "$arr_id\[$1\]";
                $job_id = $1;
            }

            if ( scalar keys %$job) { $$info{$$job{arr_id}}{$$job{job_id}} = $job; }
            $job = { job_id=>$job_id, arr_id=>$arr_id, lsf_id=>$lsf_id, cpus=>1 };

            my $job_info = $lines[$i];
            chomp($job_info);
            $i++;

            while ( $i<@lines && $lines[$i]=~/^\s{21}(.*)$/ )
            {
                $job_info .= $1;
                chomp($job_info);
                $i++;
            }
            if ( !($job_info=~/,\s*Status <([^>]+)>/) ) { confess("Could not determine the status: [$job_info]"); }
            $$job{status} = $1;
            if ( !($job_info=~/,\s*Queue <([^>]+)>/) ) { confess("Could not determine the queue: [$job_info]"); }
            $$job{queue} = $1;
            if ( !($job_info=~/,\s*Command <([^>]+)>/) ) { confess("Could not determine the command: [$job_info]"); }
            $$job{command} = $1;
        }

        if ( !defined $job ) { next; }

        # Collect also checkpoint data for LSFCR to avoid code duplication: checkpoint directory, memory, status
        # Wed Mar 19 10:14:17: Submitted from host <vr-2-2-02>...
        if ( $lines[$i]=~/^\w+\s+\w+\s+\d+ \d+:\d+:\d+:\s*Submitted from/ )
        {
            my $job_info = $lines[$i];
            chomp($job_info);

            while ( ($i+1)<@lines && $lines[$i+1]=~/^\s{21}(.*)$/ )
            {
                $i++;
                $job_info .= $1;
                chomp($job_info);
            }
            if ( $job_info=~/,\s*Checkpoint directory <([^>]+)>/ ) { $$job{chkpnt_dir} = $1; }
            if ( $job_info=~/\srusage\[mem=(\d+)/ )
            {
                $$job{mem_usage} = $1 / $$self{lsf_limits_scale};   # convert to MB
            }
        }
        # elsif ( $lines[$i]=~/^\w+\s+\w+\s+\d+ \d+:\d+:\d+:\s*Completed <exit>; TERM_CHKPNT/ )
        # {
        #     $$job{status} = 'EXIT';
        # }

        # Tue Mar 19 13:00:35: [685] started on <uk10k-4-1-07>...
        # Tue Dec 24 13:12:00: [1] started on 8 Hosts/Processors <8*vr-1-1-05>...
        # Fri Nov 15 00:55:36: Started 1 Task(s) on Host(s) <bc-25-1-08>, Allocated 1 Slo
        elsif ( $lines[$i]=~/^\w+\s+(\w+)\s+(\d+) (\d+):(\d+):(\d+):.*started on/i )
        {
            $$job{started} = date_to_time(month=>$months{$1}, day=>$2, hour=>$3, minute=>$4, year=>$year);
        }
        elsif ( $lines[$i]=~/^\w+\s+(\w+)\s+(\d+) (\d+):(\d+):(\d+):.+ dispatched to/ )    # associated with underrun status
        {
            $$job{started} = date_to_time(month=>$months{$1}, day=>$2, hour=>$3, minute=>$4, year=>$year);
        }
        elsif ( $lines[$i]=~/^\w+\s+(\w+)\s+(\d+) (\d+):(\d+):(\d+):.*started \d+ task/i )
        {
            $$job{started} = date_to_time(month=>$months{$1}, day=>$2, hour=>$3, minute=>$4, year=>$year);
        }

        # Tue Mar 19 13:58:23: Resource usage collected...
        elsif ( $lines[$i]=~/^\w+\s+(\w+)\s+(\d+) (\d+):(\d+):(\d+):\s+Resource usage collected/ )
        {
            if ( !exists($$job{started}) ) { confess("Could not parse the `bjobs -l` output for the job $$job{lsf_id}\nThe current line:\n\t$lines[$i]\nThe complete output:\n", @lines); }
            $$job{wall_time} = date_to_time(month=>$months{$1}, day=>$2, hour=>$3, minute=>$4, year=>$year) - $$job{started};
            if ( !exists($$job{cpu_time}) or $$job{cpu_time} < $$job{wall_time} ) { $$job{cpu_time} = $$job{wall_time}; }
        }
        if ( $lines[$i]=~/The CPU time used is (\d+) seconds./ )
        {
            if ( !exists($$job{cpu_time}) or $$job{cpu_time} < $1 ) { $$job{cpu_time} = $1; }
        }
        if ( $lines[$i]=~/started on (\d+) Hosts\/Processors/ )
        {
            $$job{cpus} = $1;
        }
        if ( $lines[$i]=~/Exited with exit code (\d+)\./ )
        {
            $$job{exit_code} = $1;
        }
    }
    if ( scalar keys %$job)
    {
        if ( $$job{command}=~/^cr_restart/ && exists($$job{exit_code}) && $$job{exit_code} eq '16' )
        {
            # temporary failure (e.g. pid in use) of cr_restart, ignore this failure
            return $info;
        }
        $$info{$$job{arr_id}}{$$job{job_id}} = $job;
    }
    return $info;
}

sub _check_job
{
    my ($self,$job,$jids_file) = @_;
    my $status = $$self{lsf_status_codes};
    if ( !exists($$status{$$job{status}}) )
    {
        confess("Todo: $$job{status} $$job{lsf_id}\n");
    }
    $$job{status} = $$status{$$job{status}};
    if ( $$job{status}==$$self{Running} )
    {
        if ( !exists($$job{cpu_time}) ) { $$job{cpu_time} = 0; }
        if ( !exists($$job{wall_time}) ) { $$job{wall_time} = 0; }

        # Estimate how long it might take before we are called again + plus 5 minutes to be safe, and
        # bswitch to a longer queue if necessary.

        my $wakeup_interval = $$self{limits}{wakeup_interval} ? $$self{limits}{wakeup_interval} + 300 : 300;
        my $cpu_time_mins  = ($$job{cpu_time} / $$job{cpus} + $wakeup_interval) / 60.;
        my $wall_time_mins = ($$job{wall_time} + $wakeup_interval) / 60.;
        my $time_mins = $cpu_time_mins > $wall_time_mins ? $cpu_time_mins : $wall_time_mins;
        my $new_queue = $self->_get_queue($time_mins);
        my $cur_queue = $$job{queue};
        if ( defined $new_queue && $new_queue ne $cur_queue && $$self{queue_limits}{$new_queue} > $$self{queue_limits}{$cur_queue} )
        {
            warn("Switching job $$job{lsf_id} from queue $cur_queue to $new_queue\n");
            `bswitch $new_queue '$$job{lsf_id}'`;
            if ( $? ) { warn("Could not switch queues: $$job{lsf_id}"); }
            else { $$job{queue} = $new_queue; }
        }
    }
}

# time is in minutes
sub _get_queue
{
    my ($self,$time) = @_;
    $time *= 1.1;   # increase the running time by 10% to allow more time for bswitch if the queues are full
    my $queue = exists($$self{limits}{queue}) ? $$self{limits}{queue} : $$self{default_limits}{queue};
    if ( $$self{queue_limits}{$queue} >= $time ) { return $queue; }
    for my $q (sort {$$self{queue_limits}{$a} <=> $$self{queue_limits}{$b}} keys %{$$self{queue_limits}})
    {
        if ( $time > $$self{queue_limits}{$q} ) { next; }
        return $q;
    }
    return undef;
}

sub _parse_output
{
    my ($self,$jid,$output) = @_;

    my $fname = "$output.$jid.o";
    if ( !-e $fname ) { return undef; }

    # This original version leads to a problem with stalled jobs that have no record in bjobs
    # and their LSF output file is empty - they would never be rescheduled.
    #
    #   # if the output file is empty, assume the job is running
    #   my $out = { status=>$$self{Running} };
    #
    # Instead, let's try what happens if we assume they are not running. This assumes:
    #   - a job that was just submitted will be immediately present via bjobs
    #   - a job that finished is visible via bjobs until the output is visible on the file system
    #   - this routine is called only when there is no bjobs record
    #
    # OK, this was not a good idea. Nodes can sometime appear dead (stalled mounts, for example)
    # and later corrupt existing files. Make this configurable for people who want to live
    # dangerously, but return the default to the original cautious behavior.
    #
    my $out = exists($ENV{LSF_UNLISTED_IS_DEAD}) ? { status=>$$self{No} } : { status=>$$self{Running} };

    # collect command lines and exit status to detect non-critical
    # cr_restart exits
    my @attempts = ();
    my $mem_killed = 0;
    my $cpu_killed = 0;

    open(my $fh,'<',$fname) or confess("$fname: $!");
    while (my $line=<$fh>)
    {
        # Subject: Job 822187: <_2215_1_graphs> Done
        if ( $line =~ /^Subject: Job.+\s+(\S+)$/ )
        {
            if ( $1 eq 'Exited' ) { $$out{status} = $$self{Error}; $$out{nfailures}++; }
            if ( $1 eq 'Done' ) { $$out{status} = $$self{Done}; $$out{nfailures} = 0; }
        }
        if ( $line =~ /^# LSBATCH:/ )
        {
            $line = <$fh>;
            my $cmd = substr($line,0,10);
            push @attempts, { cmd=>$cmd };
            next;
        }
        if ( $line =~ /^Exited with exit code (\d+)\./ )
        {
            if ( !scalar @attempts or exists($attempts[-1]{exit}) ) { warn("Uh, unable to parse $output.$jid.o\n"); next; }
            $attempts[-1]{exit} = $1;
        }
        # Do not count checkpoint and owner kills as a failure.
        if ( $line =~ /^TERM_CHKPNT/ && $$out{nfailures} ) { $$out{nfailures}--; }
        if ( $line =~ /^TERM_OWNER/ && $$out{nfailures} ) { $$out{nfailures}--; }
        if ( $line =~ /^TERM_MEMLIMIT:/) { $mem_killed = 1; next; }
        if ( $line =~ /^TERM_RUNLIMIT:/) { $cpu_killed = 1; next; }
        if ( $line =~ /^\s+CPU time\s+:\s+(\S+)\s+(\S+)/)
        {
            if ( $2 ne 'sec.' ) { confess("Unexpected runtime line: $line"); }
            my $time = $1 / 60;     # minutes
            if ( !exists($$out{runtime}) or $$out{runtime}<$time ) { $$out{runtime} = $time; }
        }
        if ( $line =~ /^\s+Max Memory\s+:\s+(\S+)\s+(\S+)/)
        {
            my $mem = $1;
            if ($2 eq 'KB') { $mem /= 1024; }
            elsif ($2 eq 'GB') { $mem *= 1024; }
            if ( !exists($$out{memory}) or $$out{memory}<$mem ) { $$out{memory} = $mem; }
        }
    }
    close($fh);
    for (my $i=0; $i<@attempts; $i++)
    {
        # cr_restart exited with a non-critical error
        if ( $attempts[$i]{cmd} eq 'cr_restart' && exists($attempts[$i]{exit}) && $attempts[$i]{exit} eq '16' )
        {
            $$out{nfailures}--;
        }
    }
    if ( $mem_killed ) { $$out{MEMLIMIT} = $$out{memory}; }
    if ( $cpu_killed ) { $$out{RUNLIMIT} = $$out{runtime}; }
    return $out;
}

sub past_limits
{
    my ($self,$task) = @_;
    my %out = ();
    if ( exists($$task{MEMLIMIT}) )
    {
        $out{MEMLIMIT} = $$task{MEMLIMIT};
    }
    if ( exists($$task{memory}) )
    {
        $out{memory} = $$task{memory};
    }
    if ( exists($$task{TIMELIMIT}) )
    {
        $out{RUNLIMIT} = $$task{RUNLIMIT};
    }
    if ( exists($$task{runtime}) )
    {
        $out{runtime} = $$task{runtime};
    }
    if ( exists($out{memory}) && $out{memory} < $$self{default_limits}{memory} )
    {
        $out{memory} = $$self{default_limits}{memory};
    }
    return %out;
}

sub _get_lsf_limits_unit
{
    my ($self) = @_;
    my @units = grep { /LSF_UNIT_FOR_LIMITS/ } `lsadmin showconf lim 2>/dev/null`;
    if ( $? or !@units ) { return undef; }
    my $units;
    if ( $units[0]=~/\s+MB$/i ) { $units = 'MB'; }
    elsif ( $units[0]=~/\s+kB$/i ) { $units = 'kB'; }
    elsif ( $units[0]=~/\s+GB$/i ) { $units = 'GB'; }
    else { $self->throw("Could not parse output of `lsadmin showconf lim`: $units[0]"); }
    return $units;
}
sub _set_lsf_limits_unit
{
    my ($self) = @_;
    my $units;
    if ( !exists($$self{lsf_limits_unit}) or lc($$self{lsf_limits_unit}) eq 'ask' )
    {
        my $i;
        for ($i=2; $i<15; $i++)
        {
            $units = $self->_get_lsf_limits_unit();
            if ( !defined $units )
            {
                # lasdmin may be temporarily unavailable and return confusing errors:
                # "Bad host name" or "ls_gethostinfo(): A socket operation has failed: Address already in use"
                print STDERR "lsadmin failed, trying again in $i sec...\n";
                sleep $i;
                next;
            }
            last;
        }
        if ( $i==15 ) { $self->throw("lsadmin showconf lim failed repeatedly"); }
    }
    else
    {
        $units = $self->_get_lsf_limits_unit();
        if ( !defined $units )
        {
            $units = $$self{lsf_limits_unit};
            print STDERR "Note: `lsadmin showconf lim` failed, assuming the default unit is $units\n";
        }
    }
    my $scale = 1;
    if ( lc($units) eq 'mb' ) { $scale = 1; }
    elsif ( lc($units) eq 'kb') { $scale = 1e3; }
    elsif ( lc($units) eq 'gb' ) { $scale = 1e-3; }
    else { error("Unknown unit: $units"); }
    $$self{lsf_limits_scale} = $scale;      # for conversion to MB
}

sub _create_bsub_opts_string
{
    my ($self) = @_;

    # Set bsub options. By default request 1GB of memory, the queues require mem to be set explicitly
    my $bsub_opts = '';
    my $mem    = $$self{limits}{memory} ? int($$self{limits}{memory}) : $$self{default_limits}{memory};
    my $lmem   = $$self{lsf_limits_scale} * $mem;

    my $runtime = $$self{limits}{runtime} ? $$self{limits}{runtime} : $$self{default_limits}{runtime};
    my $queue   = $self->_get_queue($runtime);
    if ( !defined $queue ) { $queue = $$self{default_limits}{queue}; }

    $bsub_opts  = sprintf " -M%d -R 'select[type==X86_64 && mem>%d] rusage[mem=%d]'", $lmem,$mem,$mem;
    $bsub_opts .= " -q $queue";
    $bsub_opts .=  exists($$self{limits}{custom}) ? ' '.$$self{limits}{custom} : '';    # custom options, such as -R avx
    if ( defined($$self{limits}{cpus}) )
    {
        $bsub_opts .= " -n $$self{limits}{cpus} -R 'span[hosts=1]'";
    }
    return $bsub_opts;
}

sub _create_bsub_ids_string
{
    my ($self,$job_name,$ids) = @_;

    # Process the list of IDs. The maximum job name length is 255 characters.
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

sub _bsub_command
{
    my ($self,$jids_file,$job_name,$bsub_cmd,$cmd) = @_;

    print STDERR "$bsub_cmd\n";
    my @out = `$bsub_cmd`;
    if ( scalar @out!=1 || !($out[0]=~/^Job <(\d+)> is submitted/) )
    {
        my $cwd = `pwd`;
        confess("Expected different output from bsub. The command was:\n\t$cmd\nThe bsub_command was:\n\t$bsub_cmd\nThe working directory was:\n\t$cwd\nThe output was:\n", @out);
    }

    # Write down info about the submitted command
    my $jid = $1;
    open(my $jids_fh, '>>', $jids_file) or confess("$jids_file: $!");
    print $jids_fh "$jid\t$job_name\t$bsub_cmd\n";
    close $jids_fh;
}

sub run_jobs
{
    my ($self,$job_name,$cmd,$ids) = @_;
    my $jids_file = "$job_name.jid";

    if ( !scalar @$ids ) { confess("No IDs given?? $job_name: $cmd\n"); }

    $cmd =~ s/{JOB_INDEX}/\$LSB_JOBINDEX/g;
    my $bsub_opts = $self->_create_bsub_opts_string();

    my @ids = @$ids;
    while ( @ids )
    {
        my $bsub_ids = $self->_create_bsub_ids_string($job_name,\@ids);

        # Do not allow the system to requeue jobs automatically, we would loose track of the job ID: -rn
        my $bsub_cmd  = qq[bsub -rn -J '${job_name}[$bsub_ids]' -e $job_name.\%I.e -o $job_name.\%I.o $bsub_opts '$cmd'];

        # Submit to LSF
        $self->_bsub_command($jids_file,$job_name,$bsub_cmd,$cmd);
    }
}

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


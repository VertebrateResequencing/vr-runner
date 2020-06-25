=head1 NAME

Runner::Chunk - create a list of regions

=head1 SYNOPSIS
    
    use Runner::Chunk;

    # create list of chunks
    my @files = 
    (
        { vcf=>'a.vcf', cache=>'a.vcf.chunks' },
        { vcf=>'b.vcf', cache=>'b.vcf.chunks' },
    );
    my $chunks = $self->Runner::Chunk::list( size_bp=>1e6, files=>\@files );
    for my $file (@files)
    {
        for my $chunk (@{$$file{chunks}})
        {
            print "$$file{vcf} $$chunk{chr} $$chunk{beg} $$chunk{end}\n";
        }
    }

    # list chromosomes
    my $chrs = Runner::Chunk::list_chrs( vcf=>['file.bcf'], cache=>'chrs.txt' );
    for my $chr (sort keys %$chrs)
    {
        for my $file (sort keys %{$$chrs{$chr}})
        {
            print "$chr\t$file\n";
        }
    }

    # split files by number of samples
    my $chunks = $self->Runner::Chunk::split_samples( nsmpl=>100, vcf=>\@list, cache=>'chunks.txt', dir=>'dir' );
    for my $chunk (@$chunks)
    {
        print "$$chunk{dst_dir}/$$chunk{dst_file}\n";
    }

    # merge VCF/BCF back
    my @merge = 
    (
        { vcf=>['a.bcf','b.bcf'], fmt=>'vcf' },   # merge a,b using `bcftools merge`, create vcf.gz
        { vcf=>['c.bcf','d.bcf'], fmt=>'bcf' },   # merge c,d using `bcftools merge`, create bcf
    );
    my $merged = $self->Runner::Chunk::merge_samples( files=>\@merge, dir=>"$outdir/merged" );

    # concatenate VCF/BCF (naive concat, no checking is performed)
    my @concat = 
    (
        { list=>['a.bcf','b.bcf','c.bcf'] }
        { list=>['a.vcf.gz','b.vcf.gz','c.vcf.gz'] }
    );
    my $concated = $self->Runner::Chunk::concat( files=>\@concat, dir=>"$outdir/merged" );

=head1 DESCRIPTION

This is a helper module for runner pipelines which creates a list of regions
based on the supplied VCF file.

Note that:

    - this version works with VCFs only, but can be extended to other formats
      as well.

    - until CSIv2 is available, we jump through the template file to determine
      non-empty regions.

=cut


package Runner::Chunk;

use strict;
use warnings;
use Carp;


=head1 METHODS

=cut

=head2 list_chrs

    About : Creates a list of chromosomes or retrieves the list from cache
    Args  : <bcftools>
                Sets bcftools binary if different from "bcftools"
            <cache>
                File to save the chunks
            <vcf>
                List of VCF/BCF files
            <regions>
                Hash of regions to include

    Returns a hash with chromosome names as keys and file names as values

=cut

sub list_chrs
{
    my (%args) = @_;

    # be ready for regions both like 'X' and 'X:154931044-155270560'
    my %reg2chr = ();
    if ( exists($args{regions}) )
    {
        for my $reg (keys %{$args{regions}})
        {
            if ( $reg=~/^([^:]+):/ ) { $reg2chr{$reg} = $1; }
        }
    }

    if ( exists($args{cache}) && -e $args{cache} )
    {
        my @lines = `cat $args{cache}`;
        if ( $? ) { confess("cat $args{cache} failed .. $!\n"); }
        my %regs = ();
        for my $line (@lines)
        {
            chomp($line);
            my ($reg,$file) = split(/\t/,$line);
            if ( exists($args{regions}) && !exists($args{regions}{$reg}) ) { next; }
            $regs{$reg}{$file} = 1;
        }
        return \%regs;
    }

    if ( !exists($args{bcftools}) ) { $args{bcftools} = 'bcftools'; }
    if ( !exists($args{vcf}) ) { confess("Missing argument: vcf\n"); }

    my %regs = ();
    for my $vcf (@{$args{vcf}})
    {
        if ( ! -e $vcf ) { confess("No such file: $vcf\n"); }

        my $cmd = "$args{bcftools} index -s $vcf";    # get the list of chromosomes
        my @lines = `$cmd`;
        if ( $? ) { confess("Error occurred: $cmd .. $!\n"); }
        my %chrs = ();
        for my $line (@lines)
        {
            my ($chr,$size,$nrec) = split(/\t/,$line);
            chomp($nrec);
            if ( !$nrec ) { next; }
            if ( exists($args{regions}) ) { $chrs{$chr} = 1; }
            else { $regs{$chr}{$vcf} = 1; }
        }
        if ( exists($args{regions}) )
        {
            for my $reg (keys %{$args{regions}})
            {
                my $chr = exists($reg2chr{$reg}) ? $reg2chr{$reg} : $reg;
                if ( !exists($chrs{$chr}) ) { next; }

                if ( !exists($reg2chr{$reg}) ) { $regs{$reg}{$vcf} = 1; }   # whole chromosome
                else 
                {
                    my $cmd = "$args{bcftools} query -f'%POS\\n' $vcf -r $reg";
                    open(my $fh,"$cmd |") or confess("$cmd: $!");
                    my $pos = <$fh>;
                    close($fh);
                    if ( defined $pos ) { $regs{$reg}{$vcf} = 1; }  # the region is not empty
                }
            }
        }
    }
    open(my $fh,'>',$args{cache}) or confess("$args{cache}: $!");
    for my $reg (sort keys %regs) 
    { 
        for my $file (sort keys %{$regs{$reg}})
        {
            print $fh "$reg\t$file\n"; 
        }
    }
    close($fh) or confess("close failed: $!");
    return \%regs;
}

=head2 list

    About : Creates a list of chunks or retrieves the list from cache
    Args  : <bcftools>
                Sets bcftools binary if different from "bcftools"
            <files>
                List of hashes defining the jobs:
                    vcf     .. template vcf/bcf to define the chunks
                    cache   .. file name with chunks
            <size_bp>
                The requested chunk size, the default is 10Mbp.
            <ngts>
                Chunk the files by the number of genotypes, files with many samples
                will be split into more parts than files with few samples
            <nsites>
                Chunk the files by the number of sites
            <regions>
                List of regions to limit the chunking to (currently only whole 
                chromosomes)

    Returns list of input hashes with a new key "chunks" holding the keys (chr,beg,end)
        
=cut

sub list
{
    my ($runner,%args) = @_;

    if ( !exists($args{files}) ) { confess "the API changed, sorry about that!"; }
    if ( !exists($args{bcftools}) ) { $args{bcftools} = 'bcftools'; }
    if ( !exists($args{size_bp}) ) { $args{size_bp} = 10e6; }

    my $method = (exists($args{ngts}) or exists($args{nsites})) ? 'Runner::Chunk::_chunk_vcf_by_ngts' : 'Runner::Chunk::_chunk_vcf';

    my $done = 1;
    for my $file (@{$args{files}})
    {
        if ( !-e $$file{cache} ) { $done = 0; last; }
        $$file{chunks} = _read_cache($$file{cache});
    }
    if ( $done ) { return $args{files}; }

    for my $file (@{$args{files}})
    {
        if ( !exists($$file{vcf}) ) { confess("Incorrect usage, the vcf key not present\n"); }
        if ( !-e $$file{vcf} ) { confess("No such file: $$file{vcf}\n"); }

        # get the list of chromosomes
        my $cmd = "$args{bcftools} index -s $$file{vcf}";
        my @chroms = `$cmd`;
        if ( $? ) { confess("Error occurred: $cmd .. $!\n"); }

        my %keep_chr = ();
        if ( exists($args{regions}) )
        {
            for my $reg (@{$args{regions}}) { $keep_chr{$reg} = 1; }
        }

        my @chunks = ();
        for my $chr (@chroms)
        {
            my @items = split(/\s+/,$chr);
            $chr = $items[0];
            if ( exists($args{regions}) && !exists($keep_chr{$chr}) ) { next; }
            $runner->spawn($method,"$$file{cache}.$chr",$chr,%args,vcf=>$$file{vcf});
            push @chunks,"$$file{cache}.$chr";
        }
        $$file{chunk_files} = \@chunks;
    }
    $runner->wait;

    for my $file (@{$args{files}})
    {
        my @chunks = ();
        for my $chunk_file (@{$$file{chunk_files}})
        {
            my $tmp = _read_cache($chunk_file);
            if ( @$tmp ) { push @chunks, @$tmp; }
        }
        $$file{chunks} = \@chunks;
        _write_cache(\@chunks,cache=>$$file{cache});

        for my $chunk_file (@{$$file{chunk_files}}) { unlink($chunk_file); }
        delete($$file{chunk_files});
    }
    return $args{files};
}

sub _chunk_vcf_by_ngts
{
    my ($self,$outfile,$chr,%args) = @_;

    my $max_nrec;
    if ( exists($args{nsites}) ) { $max_nrec = $args{nsites}; }
    else
    {
        my @smpl = `$args{bcftools} query -l $args{vcf}`;
        if ( $? ) { confess("Error: $args{bcftools} query -l $args{vcf}: $!"); }
        $max_nrec = scalar @smpl ? $args{ngts} / scalar @smpl : 1;
    }
    if ( $max_nrec < 1 ) { $max_nrec = 1; }

    my ($beg, $end);
    my $nrec = 0;
    my $chunks = [];
    my $cmd = "$args{bcftools} query -f'%POS\\n' $args{vcf} -r $chr";
    open(my $fh,"$cmd |") or confess("$cmd: $!");
    while (my $line=<$fh>)
    {
        chomp($line);
        if ( !defined $beg ) { $beg = $line; }
        $end = $line;
        if ( ++$nrec < $max_nrec ) { next; }
        chomp($beg);
        chomp($end);
        push @${chunks}, { chr=>$chr, beg=>$beg, end=>$end, nrec=>$nrec };
        $nrec = 0;
        $beg  = undef;
    }
    close($fh) or confess("close failed: $cmd");

    if ( $nrec )
    {
        push @${chunks}, { chr=>$chr, beg=>$beg, end=>$end, nrec=>$nrec };
    }

    if ( @$chunks > 1 && $$chunks[-1]{nrec} < 0.2*$max_nrec )
    {
        my $rec = pop(@$chunks);
        $$chunks[-1]{end}   = $$rec{end};
        $$chunks[-1]{nrec} += $$rec{nrec};
    }
    _write_cache($chunks,cache=>$outfile);
}

sub _chunk_vcf
{
    my ($self,$outfile,$chr,%args) = @_;

    my $chunks = [];
    if ( $args{size_bp} < 10e6 ) { $chunks = _chunk_vcf_stream($chr,%args); }
    else
    {
        my $start = 1;
        my $end   = 2147483647;    # INT_MAX

        while ( $start < $end )
        {
            my $cmd = "$args{bcftools} query -f'%POS\\n' $args{vcf} -r $chr:$start-";
            print STDERR "$cmd\n";
            open(my $fh,"$cmd |") or confess("$cmd: $!");
            my $pos = <$fh>;
            if ( !defined $pos ) { last; }
            chomp($pos);
            close($fh);

            $pos += int($args{size_bp});
            push @$chunks, { chr=>$chr, beg=>$start, end=>$pos };
            $start = $pos+1;
        }
        if ( scalar @$chunks ) { $$chunks[-1]{end} = $end; }
    }
    _write_cache($chunks,cache=>$outfile);
}

# The chunks are too small (based on a very naive heuristics), better to stream
# rather then index-jump. This needs to be changed in future with CSIv2 indexes.
sub _chunk_vcf_stream
{
    my ($chr,%args) = @_;

    my $start  = 1;
    my $end    = 2147483647;    # INT_MAX
    my $chunks = [];

    my $cmd = "$args{bcftools} query -f'%POS\\n' $args{vcf} -r $chr";
    print STDERR "$cmd\n";
    open(my $fh,"$cmd |") or confess("$cmd: $!");
    while (my $pos = <$fh>)
    {
        chomp($pos);
        if ( $pos-$start >= $args{size_bp} )
        {
            push @$chunks, { chr=>$chr, beg=>$start, end=>$pos };
            $start = $pos+1;
        }
    }
    close($fh);
    if ( scalar @$chunks ) { $$chunks[-1]{end} = $end; }
    return $chunks;
}

sub _write_cache
{
    my ($chunks,%args) = @_;
    if ( !exists($args{cache}) ) { return; }
    if ( $args{cache}=~m{/[^/]+$} ) { my $dir = $`; `mkdir -p $dir`; }
    open(my $fh,'>',"$args{cache}.part") or confess("Cannot write $args{cache}.part: $!");
    for my $chunk (@$chunks)
    {
        print $fh "$$chunk{chr}\t$$chunk{beg}\t$$chunk{end}\n";
    }
    close($fh) or confess("close error: $args{cache}.part");
    rename("$args{cache}.part",$args{cache}) or confess("rename $args{cache}.part $args{cache}: $!");
}

sub _read_cache
{
    my ($file) = @_;
    my $chunks = [];
    open(my $fh,'<',$file) or confess("Cannot write $file: $!");
    while (my $line=<$fh>)
    {
        if ( $line=~/^#/ ) { next; }    # skip commented lines
        if ( $line=~/^\s*$/ ) { next; } # skip empty lines
        my @vals = split(/\s+/,$line);
        if ( @vals!=3 ) { confess("Corrupted cache file: $file"); }
        chomp($vals[-1]);
        push @$chunks, { chr=>$vals[0], beg=>$vals[1], end=>$vals[2] };
    }
    close($fh) or confess("close error: $file");
    return $chunks;
}


=head2 split_samples

    About : Splits a list of VCF or BCF files by the number of samples
    Args  : <bcftools>
                Sets bcftools binary if different from "bcftools"
            <cache>
                File to save the list of output file names. The tab-delimited file
                has the following columns: src_file,start_sample,end_sample,dst_dir,dst_file
                where start_sample,end_sample are 0-based indices, inclusive.
            <nsmpl>
                The maximum number of samples in one file.
            <vcf>
                Array ref of VCF/BCF file names to split. If scalar, read the file list
                from the file.
            <dir>
                Output directory
            <write>
                Hash of items to write onto disk: 
                    vcf  .. subsample VCF files (generates dst_dir/dst_file.bcf)
                    list .. sample lists (generates dst_dir/dst_file.samples)

    Returns array ref of hashes with the keys
        - src_file  .. name of the source file
        - ibeg,iend .. start_sample and end_sample
        - dst_dir   .. directory
        - dst_file  .. unique chunk file name (the directory part stripped off, no suffix)
        
=cut

sub split_samples
{
    my ($runner,%args) = @_;

    if ( exists($args{cache}) && -e $args{cache} ) { return _read_split_cache($args{cache}); }

    if ( !exists($args{vcf}) ) { confess("Missing argument: vcf\n"); }
    if ( !exists($args{bcftools}) ) { $args{bcftools} = 'bcftools'; }
    if ( !exists($args{nsmpl}) ) { $args{nsmpl} = 1000; }

    if ( ref($args{vcf}) ne 'ARRAY' )
    {
        open(my $fh,'<',$args{vcf}) or confess("$args{vcf}: $!");
        my @files = grep { chomp } <$fh>;
        close($fh) or confess("close failed: $args{vcf}");
        $args{vcf} = [ @files ];
    }

    my @chunks = ();
    for my $vcf (@{$args{vcf}})
    {
        push @chunks, _split_by_sample(%args,vcf=>$vcf,nchunks=>scalar @chunks);
    }

    my $ret = scalar @chunks ? \@chunks : undef;
    if ( !$args{write} or (!$args{write}{vcf} && !$args{write}{list}) ) { return $ret; }

    for my $chunk (@chunks)
    {
        my $outfile  = "$$chunk{dst_dir}/$$chunk{dst_file}.bcf";
        my $smplfile = "$$chunk{dst_dir}/$$chunk{dst_file}.samples";
        `mkdir -p $$chunk{dst_dir}`;
        open(my $fh,'>',$smplfile) or confess("$smplfile: $!");
        print $fh join("\n",@{$$chunk{samples}}),"\n";
        close($fh) or confess("close failed; $smplfile");
        if ( !$args{write}{vcf} ) { next; }
        my $cmd =
            "$args{bcftools} view -Ob -o $outfile.part -S $smplfile $$chunk{src_file} && " .
            "$args{bcftools} index $outfile.part && " .
            "mv $outfile.part.csi $outfile.csi && " . 
            "mv $outfile.part $outfile";
        $runner->spawn('Runner::Chunk::run_cmd',$outfile,$cmd);
        delete($$chunk{samples});
    }
    $runner->wait;

    if ( !exists($args{cache}) ) { return $ret; }

    if ( $args{cache}=~m{/[^/]+$} ) { my $dir = $`; `mkdir -p $dir`; }
    open(my $fh,'>',"$args{cache}.part") or confess("Cannot write $args{cache}.part: $!");
    for my $chunk (@chunks)
    {
        print $fh "$$chunk{src_file}\t$$chunk{ibeg}\t$$chunk{iend}\t$$chunk{dst_dir}\t$$chunk{dst_file}\n";
    }
    close($fh) or confess("close error: $args{cache}.part");
    rename("$args{cache}.part",$args{cache}) or confess("rename $args{cache}.part $args{cache}: $!");

    return $ret;
}
sub _read_split_cache
{
    my ($fname) = @_;
    my @chunks = ();
    open(my $fh,'<',$fname) or confess("$fname: $!");
    while (my $line=<$fh>)
    {
        chomp($line);
        my %x = ();
        ($x{src_file},$x{ibeg},$x{iend},$x{dst_dir},$x{dst_file}) = split(/\t/,$line);
        push @chunks, \%x;
    }
    close($fh) or confess("close failed: $fname");
    return \@chunks;
}
sub _random_file_name
{
    my (%args) = @_;
    my $depth = 3;    # number of nested directories to create
    my $bits  = 7;    # bits to shift (7 -> 128 items per bin) 
    my @dir = ();
    for (my $i=0; $i<$depth; $i++)
    {
        my $value = $args{n}>>($i*$bits);
        $value &= (1<<$bits)-1;
        push @dir,$value;
    }
    my $file = pop(@dir);
    return (join('/',@dir),$file);
}
sub _split_by_sample
{
    my (%args) = @_;
    my $cmd = "$args{bcftools} query -l $args{vcf}";
    my @samples = `$cmd`;
    if ( $? ) { confess("Error occurred: $cmd .. $!\n"); }
    my $n = $args{nchunks};
    my @chunks = ();
    for (my $i=0; $i<@samples; $i+=$args{nsmpl})
    {
        my $chunk = {};
        my $j = $i + $args{nsmpl} - 1;
        if ( $j >= @samples ) { $j = @samples - 1; }
        @{$$chunk{samples}} = grep { chomp } @samples[$i..$j];
        $$chunk{src_file} = $args{vcf};
        $$chunk{ibeg} = $i;
        $$chunk{iend} = $j;
        ($$chunk{dst_dir},$$chunk{dst_file}) = _random_file_name(n=>$n++);
        $$chunk{dst_dir} = "$args{dir}/$$chunk{dst_dir}";
        push @chunks,$chunk;
    }
    return @chunks;
}
sub run_cmd
{
    my ($self,$outfile,$cmd) = @_;
    $self->cmd($cmd,verbose=>1);
}


=head2 merge_samples

    About : Merges a list of VCF or BCF files
    Args  : <annotate>
                If supplied and not equal to undef, stream through annotation
                command when merging chunks
            <bcftools>
                Sets bcftools binary if different from "bcftools"
            <dir>
                Output directory
            <files>
                List of hashes defining the merge jobs:
                    vcf .. list of vcfs to be merged
                    fmt .. output format, VCF or BCF (generates dst_file.vcf.gz or dst_file.bcf)
            <size_bp>
                The requested chunk size for parallel merging, the default is 10Mbp.

    Returns the modified array <files> with the following keys added:
        - dst_dir   .. directory
        - dst_file  .. unique file name (the directory part stripped off, no suffix)
        
=cut

sub merge_samples
{
    my ($runner,%args) = @_;

    if ( !exists($args{bcftools}) ) { $args{bcftools} = 'bcftools'; }

    my @chunks = ();
    my @tasks  = @{$args{files}};
    for (my $i=0; $i<@tasks; $i++)
    {
        my $task = $tasks[$i];
        if ( !exists($$task{vcf}) ) { confess("Incorrect usage: the key vcf not present\n"); }

        my ($dir,$file) = _random_file_name(n=>$i);
        $$task{dst_dir}  = "$args{dir}/$dir";
        $$task{dst_file} = $file;
        my $prefix = "$args{dir}/$dir/$file";

        push @chunks, { vcf=>$$task{vcf}[0], cache=>"$prefix.chunks", prefix=>$prefix };
    }
    my $chunked_tasks = $runner->Runner::Chunk::list(%args, files=>\@chunks);

    for (my $i=0; $i<@tasks; $i++)
    {
        my $task   = $tasks[$i];
        my $chunks = $$chunked_tasks[$i]{chunks};
        my $prefix = $$chunked_tasks[$i]{prefix};

        # files to merge
        open(my $fh,'>',"$prefix.merge") or confess("$prefix.merge: $!");
        print $fh join("\n", @{$$task{vcf}}) ."\n";
        close($fh) or confess("close failed: $prefix.merge");

        my $fmt = $$task{fmt} && $$task{fmt} eq 'vcf' ? 'vcf.gz' : 'bcf';
        my $out = $$task{fmt} && $$task{fmt} eq 'vcf' ? '-Oz' : '-Ob';
        if ( defined $args{annotate} )
        {
            my $annot = $args{annotate};
            $annot =~ s/^\s*\|//;
            $annot =~ s/\|\s*$//;
            $out = " -Ou | $annot $out";
        }

        for my $chunk (@$chunks)
        {
            my $outfile = "$prefix/$$chunk{chr}:$$chunk{beg}-$$chunk{end}.$fmt";
            my $cmd = 
                "$args{bcftools} merge -l $prefix.merge -r $$chunk{chr}:$$chunk{beg}-$$chunk{end} $out -o $outfile.part && " .
                "$args{bcftools} index $outfile.part && " .
                "mv $outfile.part.csi $outfile.csi && " . 
                "mv $outfile.part $outfile";
            $runner->spawn('Runner::Chunk::run_cmd',$outfile,$cmd);
            push @{$$task{concat}}, $outfile;
        }
    }
    $runner->wait;

    for my $task (@tasks)
    {
        my $prefix = "$$task{dst_dir}/$$task{dst_file}";

        # files to concat
        open(my $fh,'>',"$prefix.concat") or confess("$prefix.concat: $!");
        print $fh join("\n", @{$$task{concat}}) ."\n";
        close($fh) or confess("close failed: $prefix.concat");

        my $fmt = $$task{fmt} && $$task{fmt} eq 'vcf' ? 'vcf.gz' : 'bcf';
        my $outfile = "$prefix.$fmt";

        my $cmd = 
            "$args{bcftools} concat -n -f $prefix.concat -o $outfile.part && " .
            "$args{bcftools} index $outfile.part && " .
            "mv $outfile.part.csi $outfile.csi && " . 
            "mv $outfile.part $outfile";
        $runner->spawn('Runner::Chunk::run_cmd',$outfile,$cmd);
    }
    $runner->wait;

    return $args{files};
}


=head2 concat

    About : Concatenate a list of VCF or BCF files
    Args  : <bcftools>
                Sets bcftools binary if different from "bcftools"
            <dir>
                Output directory
            <files>
                List of hashes defining the merge jobs:
                    list .. list of VCFs or BCFs to be merged
                    fmt  .. output format, VCF or BCF (generates dst_file.vcf.gz or dst_file.bcf)

    Returns the modified array <files> with the following keys added:
        - dst_dir   .. directory
        - dst_file  .. unique file name (the directory part stripped off, with suffix)
        
=cut

sub concat
{
    my ($runner,%args) = @_;

    if ( !exists($args{bcftools}) ) { $args{bcftools} = 'bcftools'; }

    for (my $i=0; $i<@{$args{files}}; $i++)
    {
        my $task = $args{files}[$i];
        my ($dir,$file)  = _random_file_name(n=>$i);
        $$task{dst_dir}  = "$args{dir}/$dir";
        $$task{dst_file} = $file;
        my $prefix = "$args{dir}/$dir/$file";

        # files to concat
        `mkdir -p $$task{dst_dir}`;
        open(my $fh,'>',"$prefix.concat") or confess("$prefix.concat: $!");
        print $fh join("\n", @{$$task{list}}) ."\n";
        close($fh) or confess("close failed: $prefix.concat");

        my $fmt = $$task{fmt} && $$task{fmt} eq 'vcf' ? 'vcf.gz' : 'bcf';
        my $outfile = "$prefix.$fmt";

        my $cmd = 
            "$args{bcftools} concat -n -f $prefix.concat -o $outfile.part && " .
            "$args{bcftools} index $outfile.part && " .
            "mv $outfile.part.csi $outfile.csi && " . 
            "mv $outfile.part $outfile";
        $runner->spawn('Runner::Chunk::run_cmd',$outfile,$cmd);
    }
    $runner->wait;
    return $args{files};
}


=head1 AUTHORS

Author: Petr Danecek <pd3@sanger.ac.uk>

=head1 COPYRIGHT AND LICENSE

The MIT License

Copyright (C) 2012-2016 Genome Research Ltd.

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

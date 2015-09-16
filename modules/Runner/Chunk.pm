=head1 NAME

Runner::Chunk - create a list of regions

=head1 SYNOPSIS
    
    use Runner::Chunk;
    my $chunks = Runner::Chunk::list( size_bp=>1e6, vcf=>'template.bcf', cache=>'chunks.txt' );
    for my $chunk (@$chunks)
    {
        print "$$chunk{chr} $$chunk{from} $$chunk{to}\n";
    }

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

=head2 list

    About : Creates a list of chunks or retrieves the list from cache
    Args  : <bcftools>
                Sets bcftools binary if different from "bcftools"
            <cache>
                File to save the chunks
            <size_bp>
                The requested chunk size, the default is 10Mbp.
            <vcf>
                Template VCF/BCF used for splitting

    Returns list of hashes with the keys (chr,from,to)
        
=cut

sub list
{
    my (%args) = @_;

    if ( exists($args{cache}) && -e $args{cache} )
    {
        return _read_cache($args{cache});
    }

    if ( !exists($args{vcf}) ) { confess("Missing argument: vcf\n"); }
    if ( ! -e $args{vcf} ) { confess("No such file: $args{vcf}\n"); }
    if ( !exists($args{bcftools}) ) { $args{bcftools} = 'bcftools'; }
    if ( !exists($args{size_bp}) ) { $args{size_bp} = 10e6; }

    # get the list of chromosomes
    my $cmd = "$args{bcftools} index -s $args{vcf}";
    my @chroms = `$cmd`;
    if ( $? ) { confess("Error occurred: $cmd .. $!\n"); }

    my $chunks = [];
    for my $chr (@chroms)
    {
        my @items = split(/\s+/,$chr);
        $chr = $items[0];
        my $chr_chunks = _chunk_vcf($chr,%args);
        if ( @$chr_chunks ) { push @$chunks, @$chr_chunks; }
    }

    _save_cache($chunks,%args);
    return scalar @$chunks ? $chunks : undef;
}

sub _chunk_vcf
{
    my ($chr,%args) = @_;

    if ( $args{size_bp} < 10e6 ) { return _chunk_vcf_stream($chr,%args); }

    my $chunks = [];
    
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
        push @$chunks, { chr=>$chr, from=>$start, to=>$pos };
        $start = $pos+1;
    }
    if ( scalar @$chunks ) { $$chunks[-1]{to} = $end; }
    return $chunks;
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
            push @$chunks, { chr=>$chr, from=>$start, to=>$pos };
            $start = $pos+1;
        }
    }
    close($fh);
    if ( scalar @$chunks ) { $$chunks[-1]{to} = $end; }
    return $chunks;
}

sub _save_cache
{
    my ($chunks,%args) = @_;
    if ( !exists($args{cache}) ) { return; }
    open(my $fh,'>',$args{cache}) or confess("Cannot write $args{cache}: $!");
    for my $chunk (@$chunks)
    {
        print $fh "$$chunk{chr}\t$$chunk{from}\t$$chunk{to}\n";
    }
    close($fh) or confess("close error: $args{cache}");
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
        chomp($vals[2]);
        push @$chunks, { chr=>$vals[0], from=>$vals[1], to=>$vals[2] };
    }
    close($fh) or confess("close error: $file");
    return $chunks;
}





=head1 AUTHORS

Author: Petr Danecek <pd3@sanger.ac.uk>

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

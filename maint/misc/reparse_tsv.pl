#!/usr/bin/env perl

use warnings;
use strict;
use Data::Dumper;

my $fn = './convergence_rawdata.tsv';
my $out_fn = './convergence_mismatches.csv';

open( my $in_fh, '<', $fn  )
  or die "Unable to open $fn: $!\n";

open( my $out_fh, '>', $out_fn )
  or die "Unable to open $out_fn: $!\n";

my $lead_headers = { map {( $_ => 1 )} qw( Data Cmd Impl CID ) };
my( $statz, $remaining_headers );

while( my $ln = <$in_fh> ) {
  chomp $ln;

  my %f;
  {
    no warnings 'misc'; 
    %f = split /[\t\:]/, $ln;
  }
  next if (
      $f{Chunker} eq "buzhash"
   or $f{CidVer} == 0
   or $f{Inlining} != 0
  );


  $f{CID} ||= "";

  $remaining_headers->{$_} = 1 for grep { ! exists $lead_headers->{$_} } keys %f;

  my $t = $statz->{"$f{Cmd} $f{Data}"} ||= {};
  $t->{fields} = \%f;
  $t->{cids}{$f{CID}} = $f{Impl};
}

$out_fh->print( join ",", qw( source go-ipfs js-ipfs command ), sort keys %$remaining_headers );
$out_fh->print( "\n" );

for my $cmd ( sort {
  $statz->{$b}{fields}{Data} cmp $statz->{$a}{fields}{Data}
    or
  $statz->{$a}{fields}{RawLeaves} cmp $statz->{$b}{fields}{RawLeaves}
    or
  $a cmp $b
} keys %$statz ) {

  my @cids = keys %{ $statz->{$cmd}{cids} };
  next if @cids == 1;

  die Dumper { "wtf $cmd" => $statz->{$cmd} }
    if @cids != 2;

  my $impls;
  ($impls->{go}) = grep { $statz->{$cmd}{cids}{$_} eq 'go' } @cids;
  ($impls->{js}) = grep { $statz->{$cmd}{cids}{$_} eq 'js' } @cids;

  $out_fh->printf( "%s,%s,%s,%s,%s\n",
    $statz->{$cmd}{fields}{Data},
    $impls->{go},
    $impls->{js},
    $cmd,
    join (',', map { '"' . $statz->{$cmd}{fields}{$_} . '"' } sort keys %$remaining_headers ),
  );
}

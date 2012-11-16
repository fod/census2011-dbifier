#!/usr/bin/env perl

use 5.010;
use warnings;
use autodie;
use Getopt::Long;
use Text::CSV;
use List::Util 'first';
use DBI;

my ($dictfile, $datafile, $username, $password);
my ($dbname, $host, $port) = ("census2011", "localhost", 5432);

GetOptions(
    "k=s" => \$dictfile,
    "f=s" => \$datafile,
    "d=s" => \$dbname,
    "h=s" => \$host,
    "p=i" => \$port,
    "u=s" => \$username,
    "s=s" => \$password,
);

open my $fh_dict, '<', $dictfile;
my $csv_dict = Text::CSV->new({binary=>1})
    or die Text::CSV->error_diag();

my %dict;
while (my $row = $csv_dict->getline($fh_dict)) {
    $dict{$row->[1]} = $row->[2];
}

open my $fh_data, '<', $datafile;
my $csv_data = Text::CSV->new({binary=>1})
    or die Text::CSV->error_diag();

my ($data, $geogtype);
my @cols = $csv_data->column_names($csv_data->getline($fh_data));

my $col_hash;


while (my $row = $csv_data->getline_hr($fh_data)) {

    if ( $. == 2 ) {
	$geogtype = lc $row->{GEOGTYPE};
	$geogtype = 'co' if $geogtype eq 'cty';
	# $geogdesc = $row->{GEOGDESC};

	for my $col (@cols) {
	    next if $col =~ /GEOG/;
	    my ($theme, $table, $stat) = $col =~ /^T([^_]+)_(\d)(.+)$/;
	    $theme = 'theme' . sprintf("%02d", $theme);
	    $table = $geogtype . '_t' . $table;
	    $stat =~ s/^_//; $stat =~ s/^(\d)/_$1/; $stat = lc $stat;
	    $stat =~ s/^(or)$/_or/; # 'or' is a SQL reserved word so invalid for a col name
	    $stat =~ s/^(do)$/_do/; # 'do' is a SQL reserved word so invalid for a col name

	    push @{ $col_hash->{$theme}->{$table} }, $stat;
	}
    }

    # 'GEOGID' = first field in first row of csv; not matching 'eq "GEOGID"' or m/^GEOGID/
    # -- very odd but this works around it:
    my $geogid = $row->{ (first { m/GEOGID/ } keys %$row) };
    my $geogdesc = $row->{GEOGDESC};

    my ($theme, $table, $stat);
    for my $k (keys %$row) {
	unless ($k =~ /GEOG/) {

	    # Extract theme, table & col name from CSV col header
	    ($theme, $table, $stat) = $k =~ /^T([^_]+)_(\d)(.+)$/;
	    $theme = 'theme' . sprintf("%02d", $theme);
	    $table = $geogtype . '_t' . $table;

	    # Col names - precede with '_' only if it starts with a digit
	    $stat =~ s/^_//; $stat =~ s/^(\d)/_$1/; $stat = lc $stat;
	    $stat =~ s/^(or)$/_or/; # 'or' is a SQL reserved word so invalid for a col name
	    $stat =~ s/^(do)$/_do/; # 'do' is a PostgreSQL reserved word so invalid for a col name

	    # replace colname key with sanitised dbase colname
	    $row->{$stat} = delete $row->{$k};
	}
	$data->{$theme}->{$table}->{$geogid}->{$stat} = $row->{$stat};
	#(my $geogidnum = $geogid) =~ s/^\D+//;
	($data->{$theme}->{$table}->{$geogid}->{geogid} = $geogid) =~ s/^\D+//;
	$data->{$theme}->{$table}->{$geogid}->{geogtype} = $geogtype;
	$data->{$theme}->{$table}->{$geogid}->{geogdesc} = $geogdesc;
    }
}
close $fh_data;


my $dbh = DBI->connect( "DBI:Pg:dbname=$dbname;host=$host;port=$port",
    $username, $password )
  or die "Cannot connect: $DBI::errstr";


for my $schema (keys %$data) {
    $dbh->do("CREATE SCHEMA $schema");
    my @tables = keys %{ $data->{$schema} };
    for my $table ( @tables ) {
	my $st = join ' INTEGER, ', @{ $col_hash->{$schema}->{$table} };
	$st .= ' INTEGER)';
	$dbh->do("CREATE TABLE $schema.$table (geogid VARCHAR PRIMARY KEY,
                                       geogtype VARCHAR(3),
                                       geogdesc VARCHAR," . $st);

	my @geog_colnames = (qw/geogid geogtype geogdesc/);
	my @colnames = @{$col_hash->{$schema}->{$table}};
	unshift @colnames, @geog_colnames;
	my $numvals = scalar @colnames;

	my $colstring = join ', ', @colnames;
	my $placeholders = join ', ', split('', '?' x  $numvals);
	my $sth = $dbh->prepare("INSERT INTO $schema.$table ($colstring) VALUES ($placeholders)");

	for my $area ( keys %{$data->{$schema}->{$table}} ) {
	    my @values;
	    for my $k (@colnames) {
		push @values, $data->{$schema}->{$table}->{$area}->{$k};
	    }
	    $sth->execute(@values);
	}
    }
}



#!/usr/bin/env perl
use strict;
use warnings;
use 5.010;

use DateTime::Format::ISO8601;
use ElasticSearch;
use List::MoreUtils 'uniq', 'all';
use List::Util 'max';
use Path::Class;
use Term::ProgressBar;
use Text::CSV;

{
    my $es = ElasticSearch->new(
        servers      => 'api.metacpan.org:80',
        transport    => 'httptiny',
        max_requests => 0,
        no_refresh   => 1,
    );

    if (-r 'releases.csv' && -r 'authors.csv' && -r 'dists.csv') {
        _update_files($es);
    }
    else {
        _create_files($es);
    }
}

sub _update_files {
    my ($es) = @_;

    warn "Updating data...";

    my $csv = Text::CSV->new({eol => "\n"});

    my $old_releases = $csv->getline_all(file('releases.csv')->openr);
    shift @$old_releases;
    my $old_authors  = $csv->getline_all(file('authors.csv')->openr);
    shift @$old_authors;
    my $old_dists    = $csv->getline_all(file('dists.csv')->openr);
    shift @$old_dists;

    my $author_map = {
        map {; $_->[1] => $_->[0] } @$old_authors
    };
    my $dist_map = {
        map {; $_->[1] => $_->[0] } @$old_dists
    };
    my $release_map = {
        map {; $_->[2] => 1 } @$old_releases
    };

    my $from_date = DateTime->from_epoch(
        epoch => max(map { $_->[3] } @$old_releases),
    )->iso8601;
    my $next_author = max(map { $_->[0] } @$old_authors) + 1;
    my $next_dist   = max(map { $_->[0] } @$old_dists) + 1;

    my $release_fh = file('releases.csv')->open('>>');
    my $author_fh  = file('authors.csv')->open('>>');
    my $dist_fh    = file('dists.csv')->open('>>');

    my @releases = _get_releases($es, $from_date);
    for my $r (@releases) {
        if (!$author_map->{"$r->{author}"}) {
            $author_map->{"$r->{author}"} = $next_author;
            $csv->print($author_fh, [$next_author, $r->{author}]);
            $next_author++;
        }
        if (!$dist_map->{"$r->{distribution}"}) {
            $dist_map->{"$r->{distribution}"} = $next_dist;
            $csv->print($dist_fh, [$next_dist, $r->{distribution}]);
            $next_dist++;
        }
        if (!$release_map->{"$r->{archive}"}) {
            $release_map->{"$r->{archive}"} = 1;
            $csv->print(
                $release_fh,
                _normalize_release($r, $author_map, $dist_map)
            );
        }
    }

    $release_fh->close or die "couldn't close releases.csv: $!";
    $author_fh->close  or die "couldn't close authors.csv: $!";
    $dist_fh->close    or die "couldn't close dists.csv: $!";
}

sub _create_files {
    my ($es) = @_;

    warn "Creating data...";

    my $csv = Text::CSV->new({eol => "\n"});

    my @releases = grep {
        my $keep = keys(%$_) == 4 && all { defined } values(%$_);
        if (!$keep) {
            require Data::Dumper;
            warn Data::Dumper::Dumper($_);
        }
        $keep
    } _get_releases($es);

    my @author_data  = _calculate_authors(@releases);
    my @dist_data    = _calculate_dists(@releases);

    my $author_map = {
        do { my $i = 1; map { $_ => $i++ } @author_data }
    };
    my $dist_map = {
        do { my $i = 1; map { $_ => $i++ } @dist_data }
    };

    {
        warn "writing authors.csv...";
        my $fh = file('authors.csv')->openw;
        $csv->print($fh, ['author_num', 'author_id']);
        $csv->print($fh, $_)
            for sort { $a->[0] <=> $b->[0] }
                     map { [ $author_map->{$_}, $_ ] } keys %$author_map;
        $fh->close or die "couldn't close authors.csv: $!";
    }

    {
        warn "writing dists.csv...";
        my $fh = file('dists.csv')->openw;
        $csv->print($fh, ['dist_id', 'dist_name']);
        $csv->print($fh, $_)
            for sort { $a->[0] <=> $b->[0] }
                     map { [ $dist_map->{$_}, $_ ] } keys %$dist_map;
        $fh->close or die "couldn't close dists.csv: $!";
    }

    {
        warn "writing releases.csv...";
        my $fh = file('releases.csv')->openw;
        $csv->print($fh, ['author_num', 'dist_id', 'filename', 'date']);
        $csv->print($fh, $_)
            for map { _normalize_release($_, $author_map, $dist_map) }
                    @releases;
        $fh->close or die "couldn't close releases.csv: $!";
    }
}

sub _get_releases {
    my ($es, $from_date) = @_;

    my $count = _release_count($es, $from_date);
    my $progress = Term::ProgressBar->new({
        count => $count,
        ETA   => 'linear',
    });
    $progress->message("Getting $count releases...");

    my @releases = _scroll(
        $es,
        $from_date,
        'release',
        ['distribution', 'date', 'author', 'archive'],
        $progress
    );
    return map { $_->{fields} } @releases;
}

sub _release_count {
    my ($es, $from_date) = @_;

    my $result = $es->search(
        index => 'v0',
        type  => 'release',
        query => _make_query($from_date),
        size  => 0,
    );
    return $result->{hits}{total};
}

sub _scroll {
    my ($es, $from_date, $type, $fields, $progress) = @_;

    my $size = 100;
    my $scroller = $es->scrolled_search(
        index  => 'v0',
        type   => $type,
        query  => _make_query($from_date),
        scroll => '4h',
        size   => $size,
        fields => $fields,
    );

    my @results;
    while (my @next = $scroller->next($size)) {
        push @results, @next;
        $progress->update(scalar(@results));
        sleep 2;
    }
    return @results;
}

sub _make_query {
    my ($from_date) = @_;

    my $query = { match_all => {} };
    $query = {
        filtered => {
            query => $query,
            filter => {
                range => {
                    date => { gte => $from_date },
                },
            },
        },
    } if $from_date;

    return $query;
}

sub _calculate_authors {
    my @releases = @_;
    return uniq map { $_->{author} } @releases;
}

sub _calculate_dists {
    my @releases = @_;
    return uniq map { $_->{distribution} } @releases;
}

sub _normalize_release {
    my ($release, $author_map, $dist_map) = @_;
    return [
        $author_map->{$release->{author}},
        $dist_map->{$release->{distribution}},
        $release->{archive},
        DateTime::Format::ISO8601->parse_datetime($release->{date})->epoch,
    ];
}

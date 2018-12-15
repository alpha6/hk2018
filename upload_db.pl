#!/usr/bin/perl
use v5.28;
use utf8;
use strict;
use warnings FATAL => 'all';

use DBIx::Struct;
DBIx::Struct::connect("dbi:SQLite:dbname=hk2018.db","","");

use JSON::SL;
use Data::Dumper::Concise;
my $sl = JSON::SL->new();
my $cities = {};
my $countries = {};
my $interests = {};
my $statuses = {};

open my $fh, '<', 'test_data/data/accounts_1.json' or die $!;
$sl->set_jsonpointer(["/accounts/^"]);
while (sysread($fh, my $block, 1024000)) {
    my @Chunks = unpack("(a16)*", $block);
    foreach my $chunk (@Chunks) {
        my $res = $sl->feed($chunk);
        if ($res) {
            # say "===================================================================================================";
            # say "Got a chunk of data:";
            # say Dumper($res);
            # say "===================================================================================================";
            add_record($res->{Value});

         }
    }
}

close($fh);

sub add_record {
    my $record = shift;
    my $city_id;
    my $country_id;
    my $status_id;

    my $sex_id = 0 ? $record->{'sex'} = 'f' : 1;

    # say Dumper($record->{'city'});
    # return;

    if ($record->{'city'}) {
        if (exists $cities->{$record->{'city'}}) {
            $city_id = $cities->{$record->{'city'}};
        } else {
            my $new_city = new_row('cities',  name => $record->{'city'} );
            $cities->{$new_city->name} = $new_city->id;
            $city_id = $new_city->id;
        }
    }

    if ($record->{'country'}) {
        if (exists $countries->{$record->{'country'}}) {
            $country_id = $countries->{$record->{'country'}};
        } else {
            my $row = new_row('countries',  name => $record->{'country'} );
            $countries->{$row->name} = $row->id;
            $country_id = $row->id;
        }
    }

    if (exists $statuses->{$record->{'status'}}) {
        $status_id = $statuses->{$record->{'status'}};
    } else {
        my $new_status = new_row('statuses',  name => $record->{'status'});
        $statuses->{$new_status->name} = $new_status->id;
        $status_id = $new_status->id;
    }

    #`accounts` (`id`, `sname`, `fname`, `email`, `status`, `sex`, `phone`, `birth`, `city`, `country`, `joined` )
    my $account = new_row('accounts',
        id      => $record->{'id'},
        sname   => $record->{'sname'},
        fname   => $record->{'fname'},
        email   => $record->{'email'},
        status   => $status_id,
        sex     => $sex_id,
        phone   => $record->{'phone'},
        birth   => $record->{'birth'},
        city    => $city_id,
        country => $country_id,
        joined  => $record->{'joined'},
    );

    my $account_id = $account->id;

    for my $like (@{$record->{'likes'}}) {
        new_row('likes',
            from => $account_id,
            to   => $like->{'id'},
            dt   => $like->{'ts'},
        );
    }

    for my $interest (@{$record->{'interests'}}) {
        my $interest_id;
        if (exists $interests->{$interest}) {
            $interest_id = $statuses->{$interest};
        } else {
            my $row = new_row('interests',  name => $interest);
            $interests->{$row->name} = $row->id;
            $interest_id = $row->id;
        }
        new_row('accounts_interests',
            account_id   => $account_id,
            interest_id => $interest_id
        );
    }

}

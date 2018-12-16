#!/usr/bin/perl
use v5.28;
# use utf8;
use strict;
use warnings FATAL => 'all';

use DBIx::Struct qw/connector hash_ref_slice/;
DBIx::Struct::connect("dbi:SQLite:dbname=hk2018.db","","");

use JSON::SL;
use Data::Dumper::Concise;
use Encode qw(decode encode);
use Unicode::Escape qw(escape unescape);;

my $sl = JSON::SL->new();
my $cities = {};
my $countries = {};
my $interests = {};
my $statuses = {};


open my $fh, '<:raw', 'test_data/data/accounts_1.json' or die $!;
$sl->set_jsonpointer(["/accounts/^"]);
$sl->utf8(1);
my $block_tail = "";
connector->txn(sub {
    while (sysread($fh, my $block, 1024)) {
        $block = $block_tail.$block if ($block_tail);

        $block =~ m/(.*\}\,\s\{)(.*?)$/;
        my ($body, $tail) = ($1, $2);

        # say "body [$body]";
        # say "tail [$tail]";

        if ($tail){
            $block_tail = $tail;
        }


        eval {
            $body = unescape($body);
            my $res = $sl->feed($body);
            if ($res) {
                # add_record($res->{Value});
                say Dumper($res);
            }
        };
        if ($@) {
            say "$@\n [$body]\n".'='x80;
            exit 1;
        }

        $body = undef;

    }
});
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
            my $new_city = new_row('cities',  name => encode('UTF-8', $record->{'city'}, Encode::FB_CROAK));
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
    my $account;
    eval {
        $account= new_row('accounts' =>
            sex     => $sex_id,
            city    => $city_id,
            country => $country_id,
            status   => $status_id,
            hash_ref_slice $record, qw(id sname fname email phone birth joined)
        );
    };
    if ($@) {
        die $@;
    }

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

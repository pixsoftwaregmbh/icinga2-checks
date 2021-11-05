#!/usr/bin/perl -w

# check_influxdb_query.pl, queries InfluxDB using the Flux language for a value and checks the result
# Copyright (C) 2021 Sergej Kurtin (operating@pixsoftware.de)

# This program is free software; you can redistribute it and/or
# modify it under the terms of the GNU General Public License
# as published by the Free Software Foundation; either version 2
# of the License, or (at your option) any later version.

# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.

# You should have received a copy of the GNU General Public License
# along with this program; if not, write to the Free Software
# Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301, USA.

# Version 0.0.1 - Aug/2021
# Initial creation.
# by Sergej Kurtin - operating@pixsoftware.de

# Version 0.0.2 - Aug/2021
# Changes:
#  - transformed field argument into array to hold multiple values
#  - added threshold function on how to make decision when multiple fields are provided
#  - added diff argument to calc difference for counter values inside query
#  - added bytes argument for output formating
#  - check can now handle multiple values

use warnings;
use strict;

#use Monitoring::Plugin;
use Getopt::Long::Descriptive;
use Text::CSV_XS qw(csv);
use HTTP::Tiny;
use Number::Bytes::Human qw(format_bytes);

#modules for dumping data structures
#enable/uncomment these if needed
#use Data::Dumper;
#use Data::HexDump;

use constant STATUS  => qw(OK WARNING CRITICAL UNKNOWN);
my @status=(STATUS);
my $VERSION="version 0.0.2";

# disabling buffering, we are printing the output in parts
local $| = 1;

###
# parsing arguments
#
my ($opt, $usage)=describe_options(
    '%c %o',
    #reserved nagios arguments
    ["version|V"		,"Prints the version of this script.",			{ shortcircuit => 1 }],	  
    ["help|h"			,"Prints the help message.",				{ shortcircuit => 1 }],
    ["usage|?"			,"Prints a short usage message.",			{ shortcircuit => 1 }],
    []				,
    ["timeout|t=i"		,"Specify script timeout."],
    ["host|H=s"			,"Specify the host.",					{ required => 1 }],
    ["verbose|v+"		,"Set output verbosity.",				{ default  => 0 }],
    ["warning|w=f"		,"Set warning threshold.",				{ required => 1 }],
    ["critical|c=f"		,"Set critical threshold.",				{ required => 1 }],
    [],
    #our own arguments
    ["url=s"                    ,"Set the InfluxDB URL, default: http://127.0.0.1:8086",{ default => "http://127.0.0.1:8086" }],
    ["bucket|b|d|database=s"	,"Set the InfluxDB database/bucket.",			{ required => 1 }],
    ["org|o|organization=s"	,"Set the organization.",				{ required => 1 }],
    ["measurement|m=s"		,"Specify the measurement.",				{ required => 1 }],
    ["field|f=s@"		,"Specify the field.",					{ required => 1 }],
    ["fieldcon=s"		,"Specify by which function the fields are connected."],
    ["tag|T=s%"			,"Specify additional tags as key=value pairs. Can be provided multiple times."],
    ["period|p=s"		,"The time period over which the data is checked.",	{ required => 1 }],
    ["aggregate|a=s"		,"The aggregate function. The aggregate time is the same as period."],
    ["thresfun=s"		,"Threshold function how separate values are compared against the thresholds."],
    ["diff"                     ,"Calculate difference before processing, usefull for counter values like diskio."],
    ["token=s"                  ,"InfluxDB API token.",					{ required => 1 }],
    ["bytes"                    ,"Format output as human readable byte value."],
    ["debug"                    ,"Enables debug output to STDERR, usefull for running on cli."],
    ["nofill"                   ,"Disable filling null values with 0."],
    ["no-unknown-when-empty"    ,"When no data is returned the status is set to ok."]
    );
###
# print help message and exit
#
if ($opt->help or $opt->usage){
    print($usage->text);
    exit 0;
}
#capping verbosity level to 3
$opt->verbose=3 if ($opt->verbose > 3);
my $exit_status=0;

sub max {
    ($a, $b)=@_;
    return $a if ($a >= $b);
    return $b;
}

sub check_threshold {
    my ($value)=@_;
    $exit_status=max(1, $exit_status) if sprintf("%.2f", $value) > sprintf("%.2f", $opt->warning);
    $exit_status=max(2, $exit_status) if sprintf("%.2f", $value) > sprintf("%.2f", $opt->critical);
    return;
}

sub format_result {
    my (@values)=@_;
    return join(",", map {"@{${_}}[0]=${\(format_bytes(@{${_}}[1]))}"} @values) if $opt->{bytes};
    return join(",", map {"@{${_}}[0]=@{${_}}[1]"} @values);
}

# the ? has to be appended because the library does not
my $url	    = $opt->{url}."/api/v2/query?";
my %headers = ("Authorization"	=> "Token ${\$opt->{token}}",
	       "Accept"		=> "application/csv",
	       "Content-Type"	=> "application/vnd.flux");
my %urlparams=( org => $opt->org );
###
# assembling the flux query, line by line
#
# if field-con is not set this still works but gives a warning
# for now we let this just be
my $fields=join($opt->{fieldcon}, map {" r[\"_field\"] == \"$_\" "} @{$opt->{field}});
my $query    =qq(from(bucket:"$opt->{bucket}")
|> range(start: -$opt->{period})
|> filter(fn: (r) => r["_measurement"] == "$opt->{measurement}")
|> filter(fn: (r) => $fields)
    );
if ( exists $opt->{tag} ){
    while ((my $key, my $value) = each (%{$opt->tag})){
	$query .=qq(|> filter(fn: (r) => r["$key"] == "$opt->{tag}{$key}")\n);
    }
}
$query.=qq(|> filter(fn: (r) => r["host"] == "$opt->{host}")
);
###
# filling empty values with 0
# script doesnt handle null values
#
$query.=qq(|> fill(column:"_value", value: 0)
) unless $opt->nofill;
$query.=qq(|> difference(nonNegative: false, columns: ["_value"])
) if ($opt->diff);
$query.=qq(|>aggregateWindow(every: $opt->{period}, fn: $opt->{aggregate}));
say STDERR $query if $opt->debug;

my $http	 =  HTTP::Tiny->new();
my $params	 =  $http->www_form_urlencode( \%urlparams );
my $query_result =  $http->post($url.$params, {
    content	 => $query,
    headers	 => \%headers}
    );
say STDERR $query_result->{content} if $opt->{debug};
###
# $opt->measurement + "_" + array $opt->field unrolled + "_" + hash $opt->tag as key_value
# eg: CPU + USAGE_SYSTEM + CPU + CPU_TOTAL
#
if (exists $opt->{tag}){
    print uc(join("_",
		  $opt->measurement,
		  @{ $opt->field},
		  map {"${_}_$opt->{tag}{$_}"} keys %{$opt->tag})
    );
} else {
    print uc(join("_",
	      $opt->measurement,
	      @{ $opt->field})
	);
}

my $result;
###
# API call was not successfull
#
if (! $query_result->{success}){
    $exit_status=3;
    $result=$query_result->{content};}
###
# API call returned no values eg. because the field/tag do not exist
#
elsif ((! length $query_result->{content})
	 or ($query_result->{content} =~ /^\s*$/))
{
    
    $result="Received empty answer! This happens when the query does not match any data.";
    if ($opt->no_unknown_when_empty){
	$exit_status=0;
    }else{
	$exit_status=3;
    }
}
###
# Successfull API call
#
elsif ($query_result->{success}){
    # because each line start with "," the first element is empty
    # to use a hash we would need to remove the "," from the beginning for each line
    # or we use an array, then the first element is undef
    my $aoa = csv (in => \$query_result->{content});

    ###
    # the positions are sometimes not stable, therefor we have to search for them
    #
    my ($value_index, $field_index, $stop_index, $time_index);
    for my $i (0 .. scalar @{$aoa->[0]}){
	next if ! (defined $aoa->[0][$i]);
	if    ($aoa->[0][$i] eq "_value"){		
	       $value_index=$i;			
	}					
	elsif ($aoa->[0][$i] eq "_field"){	
	       $field_index=$i;			
	}					
	elsif ($aoa->[0][$i] eq "_stop"){	
	       $stop_index=$i;			
	}					
	elsif ($aoa->[0][$i] eq "_time"){	
	       $time_index=$i;		
	}
    }
    ###
    # checking only one value
    #
    if (scalar(@$aoa) <= 3){
	$result=$aoa->[1][$value_index];
	check_threshold($result);}
    ###
    # checking multiple values
    #
    else {
	my @result_values;
	for my $line (@$aoa){
	    # in case there is an empty line that was inserted as an element
	    next if (scalar(@$line) < 2);
	    # lines with actual values start with ",_result"
	    if ($$line[1] eq "_result"
		and $$line[$stop_index] eq $$line[$time_index]){
		push @result_values, [$$line[$field_index], $$line[$value_index]];
	    }
	}
	# the values have to be sumed up for comparison
	if (defined $opt->{thresfun}
	    and $opt->{thresfun} eq "sum")
	{
	    my $value_sum += @$_[1] for @result_values;
	    check_threshold($value_sum);
	    $result=format_result(@result_values);}
	else {
	    check_threshold(@$_[1]) for @result_values;
	    $result=format_result(@result_values);
	}
    }
}
print " $status[$exit_status]: ";
print $result;
exit  $exit_status;

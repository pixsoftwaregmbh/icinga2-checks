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

#Parameter:
#InfluxDB
#-h host
#-b bucket
#-o org
#-m measurement
#-f fields
#-t tag
#-p time period
#-a aggregate function
#
#-w warning
#-c critical

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
my $VERSION="version 0.0.1";

local $| = 1;

my ($opt, $usage)=describe_options(
    '%c %o',
    #reserved nagios arguments
    ["version|V"		,"Prints the version of this script.",	{ shortcircuit => 1 }],	  
    ["help|h"			,"Prints the help message.",		{ shortcircuit => 1 }],
    ["usage|?"			,"Prints a short usage message.",	{ shortcircuit => 1 }],
    [],
    ["timeout|t=i"		,"Specify script timeout."],
    ["host|H=s"			,"Specify the host."],
    ["verbose|v+"		,"Set output verbosity.",		{ default => 0 }],
    ["warning|w=f"		,"Set warning threshold."],
    ["critical|c=f"		,"Set critical threshold."],
    [],
    #our own arguments
    ["bucket|b|d|database=s"	,"Set the InfluxDB database/bucket."],
    ["org|o|organization=s"	,"Set the organization."],
    ["measurement|m=s"		,"Specify the measurement."],
    ["field|f=s"		,"Specify the field."],
    ["tag|T=s%"			,"Specify additional tags as key=value pairs. Can be provided multiple times."],
    ["period|p=s"		,"The time period over which the data is checked."],
    ["aggregate|a=s"		,"The aggregate function. The aggregate time is the same as period."]
    );
###
# print help message and exit
#
if ($opt->help){
    print($usage->text);
    exit 0;
}
#capping verbosity level to 3
$opt->verbose=3 if ($opt->verbose>3);
my $exit_status=0;

# the ? has to be added because the library does not
my $url	    = "http://127.0.0.1:8086/api/v2/query?";
my %headers =("Authorization" => "Token <token>",
	     "Accept"	      => "application/csv",
	     "Content-Type"   => "application/vnd.flux" );
my %urlparams=( org => $opt->org );
my $query    =qq(from(bucket:"$opt->{bucket}")
|> range(start: -$opt->{period})
|> filter(fn: (r) => r["_measurement"] == "$opt->{measurement}")
|> filter(fn: (r) => r["_field"] == "$opt->{field}")
);
foreach my $key ( keys %{$opt->tag}){
    $query .=qq(|> filter(fn: (r) => r["$key"] == "$opt->{tag}{$key}")\n);
}
$query.=qq(|> filter(fn: (r) => r["host"] == "$opt->{host}")
|> aggregateWindow(every: $opt->{period}, fn: $opt->{aggregate}));
#say STDERR $query;

my $http	 =  HTTP::Tiny->new();
my $params	 =  $http->www_form_urlencode( \%urlparams );
my $query_result =  $http->post($url.$params, {
    content	 => $query,
    headers	 => \%headers}
    );
# eg: CPU_USAGE_SYSTEM__
print uc($opt->measurement."_".$opt->field)."_";
# eg: CPU_CPU_TOTAL_
print map {uc($_) . "_"} %{$opt->tag};
my $result;
if (! $query_result->{success}){
    $exit_status=3;
    $result=$query_result->{content};   
} elsif ((! length $query_result->{content})
	 or ($query_result->{content} =~ /^\s*$/)
    ){
    $exit_status=3;
    $result="Received empty answer! This happens when the query does not match any data.";
} elsif ($query_result->{success}){
    my $aoa = csv (in => \$query_result->{content});
    $result=$aoa->[1][6];
    $exit_status=1 if sprintf("%.2f", $result) > sprintf("%.2f", $opt->warning);
    $exit_status=2 if sprintf("%.2f", $result) > sprintf("%.2f", $opt->critical);
}
print " $status[$exit_status]: ";
print $result;
exit  $exit_status;

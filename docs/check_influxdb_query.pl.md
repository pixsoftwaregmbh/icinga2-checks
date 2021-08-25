# check_influxdb_query.pl

## Description
This check sends a Flux query to InfluxDB, then checks the answer against thresholds. Can be used for following scenarios:
- check single value, eg. CPU load
- check multiple values eg. disk input and output
  - check whether one value is above the thresholds
  - check whether the sum is above the thresholds

## Limitations
This check is written for very little use cases in mind. It also does not check whether the query makes sense. If your scenario is not covered adapt it according to your needs.
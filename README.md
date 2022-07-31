# multipg2csv

multipg2csv allows you to run a single query against multiple postgresql instances
simultaneously, collecting the results into a collection of csv files packaged
in a zip archive.

## Installation

From source:

    go install github.com/tardisx/multipg2csv@latest

Binaries:

... coming soon

## Usage

    multipg2csv -query 'SELECT * FROM pg_stats_all_indexes;' -output index_stats.zip postgresql://server1/db1 postgresql://server2/db2

Or maybe leverage your shell:

    multipg2csv -query 'SELECT * FROM pg_stats_all_indexes;' -output index_stats.zip postgresql://db{1..20}/somedb

PostgreSQL connection URL's are described here: https://www.postgresql.org/docs/current/libpq-connect.html#LIBPQ-CONNSTRING

Note that `multipg2csv` will use your .pgpass if available, and you should definitely use it,
instead of supplying passwords on the command line.

# Known bugs

* Quitting while transfers are still in progress does not work correctly.

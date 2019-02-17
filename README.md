# Client for working with database records

The utility works in a bash terminal environment. This utility is used to test and debug script operations when working with a database.

# Flags

| short flag | full flag  | description                                                                        |
| ----       | ----       | ----                                                                               |
| `-t`       | `--table`  | db table. '-tt' - show all tables                                                  |
| `-tw`      | `--two`    | DB table take two pictures and highlight the difference                            |
| `-w`       | `--write`  | l - print lines, t - table                                                         |
| `-s`       | `--sql`    | direct input sql query                                                             |
| `-b`       | `--build`  | create record (-t <table>) <col>::<val>//<col>::<val> (the first value is Unique!) |
| `-u`       | `--update` | update record (-t <table>) <id>//<col>::<val>//<col>::<val>                        |
| `-d`       | `--delete` | delete entry (-t <table>) <id>,<id>,<id>                                           |
| `-c`       | `--clear`  | clear the table [bool]                                                             |

# Command Examples

Arbitrary sql query

    python3 __init__.py -s "SHOW TABLES"

Show tables

    python3 __init__.py -tt

Show table "users"

    python3 __init__.py -t users -wt

Add an entry to the "orders" table

    python3 __init__.py -t orders -b id::35//name::Tom

Update entry to orders table

    python3 __init__.py -t orders -u 37//name::markkk

Delete entries in the "orders" table by id

    python3 __init__.py -t orders -d 34,35,36

Clear the "orders" table

    python3 __init__.py -t orders -c

Make two snapshots of all the tables, to compare the number of records in each before the script runs and after

    python3 __init__.py -tt -tw -wt

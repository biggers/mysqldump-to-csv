#!/usr/bin/env python
import csv
import sys
import re

# This prevents prematurely closed pipes from raising
# an exception in Python
from signal import signal, SIGPIPE, SIG_DFL
signal(SIGPIPE, SIG_DFL)

# REF: (originally)
# How-to run, one of:
# (1) unzip a .zip'd MySQL-dump, pipe to this filter
#   unzip -pa ../tm2_local_20161202.sql.zip | ./mysqldump_to_csv.py
# (2)
#   /mysqldump_to_csv.py prod_proj_sql.dump  # dump.sql, *not* compressed!

_CREAT_TBL_RE = re.compile(r"""^CREATE TABLE `(?P<table_name>.*)`""")

_INSERT_RE = re.compile(r"""^INSERT INTO `(?P<table_name>.*)` VALUES (?P<values>.*)\);$""")


def increase_csv_field_size_limit():
    """
    Increase csv field size limit
    """
    max_size = sys.maxsize
    while True:
        try:
            csv.field_size_limit(max_size)
            break
        except:
            max_size /= 10


def parse_values(values, outfile):
    """
    Given a file handle and the raw values from a MySQL INSERT
    statement, write the equivalent CSV to the file
    """
    latest_row = []

    reader = csv.reader([values], delimiter=',',
                        doublequote=False,
                        escapechar='\\',
                        quotechar="'",
                        strict=True
                        )

    writer = csv.writer(outfile, delimiter=',', quotechar='\\',
                        quoting=csv.QUOTE_MINIMAL)

    for reader_row in reader:
        for column in reader_row:

            # If our current string is empty...
            if len(column) == 0 or column == 'NULL':
                latest_row.append(chr(0))
                continue
            if column == 'NULL)':
                latest_row.append(chr(0) + ')')
                continue
            if column == 'NULL);':
                latest_row.append(chr(0) + ');')
                continue

            # If our string starts with an open paren
            if column[0] == "(":
                # Assume that this column does not begin
                # a new row.
                new_row = False

                # If we've been filling out a row
                if len(latest_row) > 0:
                    # Check if the previous entry ended in
                    # a close paren. If so, the row we've
                    # been filling out has been COMPLETED
                    # as:

                    #    1) the previous entry ended in a )
                    #    2) the current entry starts with a (
                    if latest_row[-1][-1] == ")":
                        # Remove the close paren.
                        latest_row[-1] = latest_row[-1][:-1]
                        new_row = True

                # If we've found a new row, write it out
                # and begin our new one
                if new_row:
                    writer.writerow(latest_row)
                    latest_row = []
                # If we're beginning a new row, eliminate the

                # opening parentheses.
                if len(latest_row) == 0:
                    column = column[1:]

            # Add our column to the row we're working on.
            latest_row.append(column)

        # At the end of an INSERT statement, we'll
        # have the semicolon.
        # Make sure to remove the semicolon and the close paren
        if latest_row[-1][-2:] == ");":
            latest_row[-1] = latest_row[-1][:-2]
        writer.writerow(latest_row)


def main():
    """
    Parse arguments and start the program
    """
    # Iterate over all lines from stdin
    increase_csv_field_size_limit()

    # "parse" for CREATE TABLE (re), get 'table_name'
    #  ... open table_name.csv file
    # ?? parse & write-out the CREATE_TABLE sql
    # "parse" for VALUES from INSERT

    table_name = None
    outp = None
    try:
        inp = open(sys.argv[1], 'rb')
    except IndexError:
        inp = sys.stdin            # default, 'stdin'

    try:
        # for line in fileinput.input(inp):
        for line in inp:        # MySQL dumpfile

            so = _CREAT_TBL_RE.search(line)
            if so:
                new_table = so.groups()[0]
                if table_name != new_table:
                    table_name = new_table
                    if outp:
                        outp.close()
                    outp = open("{}.csv".format(new_table), 'wb')
                continue

            so = _INSERT_RE.search(line)
            if so:
                table_name, values_raw = so.groups()
                parse_values(values_raw, outp)

    except KeyboardInterrupt:
        sys.exit(0)
    finally:
        if outp:
            outp.close()


if __name__ == "__main__":
    main()

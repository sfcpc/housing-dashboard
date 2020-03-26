# Lint as: python3
"""Generate test data from real data."""

import csv
import argparse
import re
import sys

from fileutils import open_file

csv.field_size_limit(min(2**31-1, sys.maxsize))


def filter_file(infile, outfile, pattern):
    with open_file(infile,
                   mode='rt',
                   encoding='utf-8',
                   errors='replace',
                   newline='') as inf:
        reader = csv.reader(inf)
        with open(outfile, 'w', encoding='utf-8', newline='') as outf:
            writer = csv.writer(outf, lineterminator='\n')
            # Print the columns
            writer.writerow(next(reader))
            for row in reader:
                for field in row:
                    if pattern.findall(field):
                        writer.writerow(row)
                        break


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('infile', help='Input file')
    parser.add_argument('outfile', help='Output file')
    parser.add_argument(
        'regex', help='Regex to search for in every column of every line')
    args = parser.parse_args()

    pattern = re.compile(args.regex, re.IGNORECASE)
    filter_file(args.infile, args.outfile, pattern)

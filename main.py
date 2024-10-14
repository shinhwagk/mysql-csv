import argparse
import os
import sys
from typing import Optional

import mysql.connector
import pandas as pd


class Args:
    host: str
    port: int
    user: str
    password: str
    database: Optional[str]
    query_file: str
    csv_file: Optional[str]


parser = argparse.ArgumentParser(description="Export MySQL query results to CSV file.")
parser.add_argument("--host", required=True, help="MySQL server host")
parser.add_argument("--port", required=True, type=int, help="MySQL server port")
parser.add_argument("--user", required=True, help="MySQL user")
parser.add_argument("--password", required=True, help="MySQL password")
parser.add_argument("--database", help="MySQL database name")
parser.add_argument("--query-file", required=True, help="Path to the SQL query file")
parser.add_argument("--csv-file", help="Path to save the output CSV file")

args: Args = parser.parse_args()

print(args)

csv_file = args.csv_file or f"{args.query_file}.csv"


if os.path.exists(csv_file):
    print(f"CSV file '{csv_file}' already exists.")
    sys.exit(0)


with open(args.query_file, "r") as file:
    query = file.read().strip()

rows_total = 0
with mysql.connector.connect(host=args.host, port=args.port, user=args.user, password=args.password, database=args.database, compress=True) as con:
    with con.cursor(buffered=False, dictionary=True) as cur:
        cur.execute(query)

        while True:
            rows = cur.fetchmany(10000)

            if not rows:
                break

            pd.DataFrame(rows).to_csv(csv_file, index=False, mode="a")
            rows_total += len(rows)
            print(f"Exported {len(rows)}/{rows_total} rows...")

print(f"Query '{args.query_file}' results saved to '{csv_file}'")

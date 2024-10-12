import argparse
from typing import Optional

import pandas as pd
from sqlalchemy import create_engine


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

try:
    database_str = f"/{args.database}" if args.database else ""
    engine = create_engine(f"mysql+mysqlconnector://{args.user}:{args.password}@{args.host}:{args.port}{database_str}")

    with open(args.query_file, "r") as file:
        query = file.read()

    csv_file = args.csv_file or f"{args.query_file}.csv"
    pd.read_sql(query, con=engine).to_csv(csv_file, index=False)

    print(f"Query {args.query_file} results saved to {csv_file}")
except Exception as e:
    print(f"Error while connecting to MySQL: {e}")
    exit(1)

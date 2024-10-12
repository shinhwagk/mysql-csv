import argparse

import pandas as pd
from sqlalchemy import create_engine


def mysql_query_to_csv(host, port, user, password, database, query_file_path, csv_file_path):
    try:
        engine = create_engine(f"mysql+mysqlconnector://{user}:{password}@{host}:{port}/{database}")

        with open(query_file_path, "r") as file:
            query = file.read()

        with engine.connect() as connection:
            df = pd.read_sql(query, con=connection)

        df.to_csv(csv_file_path, index=False, header=df.columns)

        print(f"Query results saved to {csv_file_path}")
    except Exception as e:
        print(f"Error while connecting to MySQL: {e}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Export MySQL query results to CSV file.")
    parser.add_argument("--host", required=True, help="MySQL server host")
    parser.add_argument("--port", required=True, type=int, help="MySQL server port")
    parser.add_argument("--user", required=True, help="MySQL user")
    parser.add_argument("--password", required=True, help="MySQL password")
    parser.add_argument("--database", required=True, help="MySQL database name")
    parser.add_argument("--query-file", required=True, help="Path to the SQL query file")
    parser.add_argument("--csv-file", required=True, help="Path to save the output CSV file")

    args = parser.parse_args()

    mysql_query_to_csv(args.host, args.port, args.user, args.password, args.database, args.query_file, args.csv_file)

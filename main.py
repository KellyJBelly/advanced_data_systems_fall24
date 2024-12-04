import sqlite3
import pandas as pd
import matplotlib.pyplot as plt
import time
import random
import os


# Function to create a new SQLite database with the same fields as the dataset
def createTSDB(filename):
    with sqlite3.connect(filename) as connect:
        cursor = connect.cursor()
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS "PowerUsage_2016_to_2020" (
                "StartDate"	TEXT NOT NULL COLLATE NOCASE,
                "Value (kWh)"	REAL NOT NULL,
                "day_of_week"	INTEGER NOT NULL,
                "notes"	TEXT NOT NULL COLLATE NOCASE
            );
        """)


# Enables Write-Ahead Logging (WAL) mode for SQLite, which allows concurrent reads and writes.
def set_wal_mode(connect):
    connect.execute("PRAGMA journal_mode=WAL;")


# Load data into the database
# def load_data_to_db(filename, db_connect):
#     if not os.path.exists(filename):
#         print(f"Error: File '{filename}' does not exist.")
#         return  # Exit the function if the file is missing

#     data = pd.read_csv(filename)
#     data['day_of_week'] = pd.to_datetime(data['StartDate']).dt.dayofweek
#     with db_connect:
#         data.to_sql('PowerUsage_2016_to_2020', db_connect, if_exists='replace', index=False)
def load_data_to_db(filename, db_connect):
    # Print the absolute path for debugging
    print(f"Looking for file: {os.path.abspath(filename)}")

    # Check if the file exists
    if not os.path.exists(filename):
        print(f"Error: File '{filename}' does not exist.")
        return  # Exit the function if the file is missing

    # Load the CSV file into a pandas DataFrame
    print(f"Loading data from '{filename}'...")
    data = pd.read_csv(filename)
    data['day_of_week'] = pd.to_datetime(data['StartDate']).dt.dayofweek  # Add the day of week column

    # Insert data into the SQLite database
    with db_connect:
        data.to_sql('PowerUsage_2016_to_2020', db_connect, if_exists='replace', index=False)
    print("Data successfully loaded into the database.")

# Example usage
import sqlite3

# Connect to the SQLite database
db_connect = sqlite3.connect('edge.db')

# Call the function with the correct file path
load_data_to_db('C:/path/to/your/dataset/power_usage_2016_to_2020.csv', db_connect)

# Partition data between edge and cloud
def partition_data(edge_connect, cloud_connect):
    set_wal_mode(edge_connect)
    set_wal_mode(cloud_connect)

    edge_cursor = edge_connect.cursor()
    cloud_cursor = cloud_connect.cursor()

    try:
        # Move data older than 12,000 rows from edge to cloud
        edge_cursor.execute("""
            SELECT * FROM PowerUsage_2016_to_2020
            ORDER BY StartDate ASC
            LIMIT -1 OFFSET 12000;
        """)
        data_to_migrate = edge_cursor.fetchall()

        # Insert migrated data into the cloud database
        for row in data_to_migrate:
            cloud_cursor.execute("""
                INSERT INTO PowerUsage_2016_to_2020 (StartDate, "Value (kWh)", day_of_week, notes)
                VALUES (?, ?, ?, ?);
            """, row)

        # Delete migrated data from the edge database
        edge_cursor.execute("""
            DELETE FROM PowerUsage_2016_to_2020
            WHERE StartDate IN (
                SELECT StartDate FROM PowerUsage_2016_to_2020
                ORDER BY StartDate ASC
                LIMIT -1 OFFSET 12000
            );
        """)
        edge_connect.commit()
        cloud_connect.commit()
    except sqlite3.Error as e:
        print(f"Error during partitioning: {e}")
    finally:
        edge_cursor.close()
        cloud_cursor.close()


# Query Survival Benchmark
def query_survival(edge_connect, cloud_connect, query):
    edge_cursor = edge_connect.cursor()
    cloud_cursor = cloud_connect.cursor()

    try:
        edge_cursor.execute(query)
        edge_results = edge_cursor.fetchall()
        if edge_results:
            return "Edge", len(edge_results)
        else:
            cloud_cursor.execute(query)
            cloud_results = cloud_cursor.fetchall()
            return "Cloud", len(cloud_results)
    except sqlite3.Error as e:
        print(f"Query error: {e}")
    finally:
        edge_cursor.close()
        cloud_cursor.close()


# Downsampling Impact Benchmark
def compare_downsampling(edge_connect, cloud_connect):
    edge_cursor = edge_connect.cursor()
    cloud_cursor = cloud_connect.cursor()

    try:
        # High-resolution data: Only query the edge database
        edge_cursor.execute("""
            SELECT AVG("Value (kWh)") FROM PowerUsage_2016_to_2020;
        """)
        edge_avg = edge_cursor.fetchone()[0]

        # Low-resolution data: Only query the cloud database
        cloud_cursor.execute("""
            SELECT AVG("Value (kWh)") FROM PowerUsage_2016_to_2020;
        """)
        cloud_avg = cloud_cursor.fetchone()[0]

        return edge_avg, cloud_avg
    finally:
        edge_cursor.close()
        cloud_cursor.close()


# Latency Simulation Benchmark
def simulate_latency(edge_connect, cloud_connect):
    query = "SELECT * FROM PowerUsage_2016_to_2020 WHERE day_of_week = 1"
    start_time = time.time()
    with edge_connect:
        edge_connect.execute(query)
    edge_time = time.time() - start_time

    start_time = time.time()
    time.sleep(random.uniform(0.1, 0.2))  # Simulate cloud latency
    with cloud_connect:
        cloud_connect.execute(query)
    cloud_time = time.time() - start_time

    return edge_time, cloud_time


# Query Throughput Benchmark
def measure_throughput(edge_connect):
    query = "SELECT * FROM PowerUsage_2016_to_2020 WHERE day_of_week = 1"
    start_time = time.time()
    query_count = 0
    while time.time() - start_time < 1:  # Run queries for 1 second
        with edge_connect:
            edge_connect.execute(query)
        query_count += 1
    return query_count


# Main execution
def main():
    # Setup databases
    createTSDB('edge.db')
    createTSDB('cloud.db')

    edge_connect = sqlite3.connect('edge.db')
    cloud_connect = sqlite3.connect('cloud.db')

    try:
        # Load data
        load_data_to_db('power_usage_2016_to_2020.csv', edge_connect)

        # Partition data
        partition_data(edge_connect, cloud_connect)

        # Run benchmarks
        print("Running Query Survival Benchmark...")
        survival_result = query_survival(edge_connect, cloud_connect, "SELECT * FROM PowerUsage_2016_to_2020 WHERE day_of_week = 1;")
        print(f"Query resolved at: {survival_result[0]} with {survival_result[1]} rows.")

        print("\nRunning Downsampling Impact Benchmark...")
        edge_avg, cloud_avg = compare_downsampling(edge_connect, cloud_connect)
        print(f"Edge Data Avg: {edge_avg}, Cloud Data Avg: {cloud_avg}")

        print("\nRunning Latency Simulation Benchmark...")
        edge_time, cloud_time = simulate_latency(edge_connect, cloud_connect)
        print(f"Edge Query Time: {edge_time:.4f}s, Cloud Query Time: {cloud_time:.4f}s")

        print("\nRunning Query Throughput Benchmark...")
        throughput = measure_throughput(edge_connect)
        print(f"Query Throughput: {throughput} queries/second")

    finally:
        edge_connect.close()
        cloud_connect.close()


if __name__ == "__main__":
    main()
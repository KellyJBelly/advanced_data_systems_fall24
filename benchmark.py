import time

def measure_query_performance(cursor, query):
    start_time = time.time()
    cursor.execute(query)
    results = cursor.fetchall()
    end_time = time.time()
    elapsed_time = end_time - start_time
    return elapsed_time

def measure_ingest_performance(connect, data_batch):
    start_time = time.time()
    for row in data_batch:
        connect.execute("INSERT INTO PowerUsage_2016_to_2020 VALUES (?, ?, ?, ?)", row)
    connect.commit()
    end_time = time.time()
    elapsed_time = end_time - start_time
    return elapsed_time

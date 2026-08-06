import time
import logging
import json
import random
import string
import sys
from unittest.mock import MagicMock

# Setup logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def generate_random_json(size_kb=1):
    """Generate a random JSON object of approximately size_kb KB."""
    data = {}
    while len(json.dumps(data)) < size_kb * 1024:
        key = ''.join(random.choices(string.ascii_letters, k=8))
        value = ''.join(random.choices(string.ascii_letters, k=100))
        data[key] = value
    return json.dumps(data)

def setup_benchmark_table(cur):
    logger.info("Setting up benchmark table...")
    cur.execute("DROP TABLE IF EXISTS benchmark_stocks")
    cur.execute("""
        CREATE TABLE benchmark_stocks (
            date DATE PRIMARY KEY,
            most_active JSONB,
            price1 JSONB,
            price2 JSONB,
            price3 JSONB,
            new1 JSONB,
            new2 JSONB,
            new3 JSONB
        )
    """)

    # Insert dummy data
    logger.info("Inserting dummy data...")
    for i in range(10): # 10 days
        date = f"2023-01-{i+1:02d}"

        # Make 'most_active' and 'new' columns large to simulate overhead
        cols = {
            "date": date,
            "most_active": generate_random_json(size_kb=50), # 50KB
            "price1": generate_random_json(size_kb=5),
            "price2": generate_random_json(size_kb=5),
            "price3": generate_random_json(size_kb=5),
            "new1": generate_random_json(size_kb=50), # 50KB
            "new2": generate_random_json(size_kb=50),
            "new3": generate_random_json(size_kb=50),
        }

        cur.execute("""
            INSERT INTO benchmark_stocks
            (date, most_active, price1, price2, price3, new1, new2, new3)
            VALUES (%(date)s, %(most_active)s, %(price1)s, %(price2)s, %(price3)s, %(new1)s, %(new2)s, %(new3)s)
        """, cols)

def benchmark_queries(cur):
    # Query 1: SELECT * (Simulating current state)
    # Fetching 10 rows with ~200KB per row = ~2MB total
    start_time = time.time()
    cur.execute("SELECT * FROM benchmark_stocks")
    rows = cur.fetchall()
    duration_star = time.time() - start_time
    logger.info(f"SELECT * duration: {duration_star:.4f} seconds")

    # Query 2: SELECT specific columns (Simulating optimized state for stg_price)
    # Fetching 10 rows with ~15KB per row = ~150KB total
    start_time = time.time()
    cur.execute("SELECT date, price1, price2, price3 FROM benchmark_stocks")
    rows = cur.fetchall()
    duration_optimized = time.time() - start_time
    logger.info(f"SELECT specific columns duration: {duration_optimized:.4f} seconds")

    if duration_star > 0:
        improvement = (duration_star - duration_optimized) / duration_star * 100
        logger.info(f"Improvement: {improvement:.2f}%")
    else:
        logger.info("Duration was 0, cannot calculate percentage.")

def main():
    try:
        from airflow.providers.postgres.hooks.postgres import PostgresHook

        # Try to connect
        try:
            hook = PostgresHook(postgres_conn_id="postgres_stock")
            conn = hook.get_conn()
            cur = conn.cursor()

            setup_benchmark_table(cur)
            benchmark_queries(cur)

            cur.close()
            conn.close()
        except Exception as e:
            logger.warning(f"Could not connect to database: {e}")
            logger.info("Skipping actual benchmark execution.")

    except ImportError:
         logger.warning("Airflow providers not installed.")

if __name__ == "__main__":
    main()

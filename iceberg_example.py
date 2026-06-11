import pyarrow as pa
import pyarrow.parquet as pq
from pyiceberg.catalog import Catalog
from pyiceberg.schema import Schema
from pyiceberg.types import IntegerType, StringType
from pyiceberg.expressions import GreaterThan
from pyiceberg.exceptions import NoSuchTableError
import os
import shutil

# --- Configuration ---
# Using a local directory as the catalog for simplicity
CATALOG_DIR = "./iceberg_catalog"
TABLE_NAME = "my_iceberg_table"

# --- Helper Functions ---
def create_catalog():
    if os.path.exists(CATALOG_DIR):
        shutil.rmtree(CATALOG_DIR)
    os.makedirs(CATALOG_DIR)
    # In a real scenario, you'd configure a proper catalog (e.g., Hive, Glue, REST)
    # For this example, we'll simulate a catalog by directly interacting with files
    # and assuming a simple file-based metadata store.
    print(f"Simulating catalog at: {CATALOG_DIR}")
    return Catalog(uri=f"file://{os.path.abspath(CATALOG_DIR)}", name="local")

def create_iceberg_table(catalog):
    # Define the schema for the Iceberg table
    schema = Schema.from_dict({
        "type": "struct",
        "fields": [
            {"id": 1, "name": "user_id", "type": IntegerType()},
            {"id": 2, "name": "username", "type": StringType()},
            {"id": 3, "name": "event_count", "type": IntegerType()}
        ]
    })

    # Create the table if it doesn't exist
    try:
        catalog.load_table(TABLE_NAME)
        print(f"Table '{TABLE_NAME}' already exists.")
    except NoSuchTableError:
        print(f"Creating table '{TABLE_NAME}'...")
        catalog.create_table(TABLE_NAME, schema)
        print(f"Table '{TABLE_NAME}' created.")

def write_data(catalog):
    table = catalog.load_table(TABLE_NAME)
    
    # Sample data
    data = [
        {'user_id': 101, 'username': 'alice', 'event_count': 5},
        {'user_id': 102, 'username': 'bob', 'event_count': 10},
        {'user_id': 101, 'username': 'alice', 'event_count': 7} # Another event for alice
    ]

    # Convert to PyArrow Table
    arrow_table = pa.Table.from_pylist(data, schema=table.schema.to_arrow_schema())

    # Append data to the Iceberg table
    print(f"Writing data to '{TABLE_NAME}'...")
    with table.new_writer() as writer:
        writer.write(arrow_table)
    print("Data written.")

def query_data(catalog):
    table = catalog.load_table(TABLE_NAME)

    # Example: Querying data where event_count > 6
    print("Querying data where event_count > 6:")
    query_expression = GreaterThan('event_count', 6)
    
    # Use the table's scanner to read data
    scanner = table.scan(predicate=query_expression)
    results = scanner.to_arrow_table()
    
    print(results)

def time_travel(catalog):
    table = catalog.load_table(TABLE_NAME)
    
    # Get the current snapshot ID
    current_snapshot_id = table.current_snapshot_id
    print(f"Current snapshot ID: {current_snapshot_id}")

    # To demonstrate time travel, we'd typically need at least two snapshots.
    # Since we only wrote data once, we can't go back to a previous state.
    # In a real scenario, after more writes, you could do:
    # previous_snapshot_id = table.history()[1].snapshot_id # Example: get the second to last snapshot
    # print(f"Attempting to read from snapshot ID: {previous_snapshot_id}")
    # scanner_time_travel = table.scan(snapshot_id=previous_snapshot_id)
    # results_time_travel = scanner_time_travel.to_arrow_table()
    # print("Data from previous snapshot:")
    # print(results_time_travel)
    print("Time travel requires multiple snapshots. Perform more writes to test.")

# --- Main Execution ---
def main():
    # 1. Create a simulated catalog
    catalog = create_catalog()

    # 2. Create an Iceberg table
    create_iceberg_table(catalog)

    # 3. Write data to the table
    write_data(catalog)

    # 4. Query data from the table
    query_data(catalog)

    # 5. Demonstrate time travel (conceptually)
    time_travel(catalog)

    print("\nExample finished. Check the './iceberg_catalog' directory for metadata.")

if __name__ == "__main__":
    main()

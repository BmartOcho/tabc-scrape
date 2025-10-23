#!/usr/bin/env python3
"""
Database inspection script to verify scraped data quality
"""

import sys
import os
import sqlite3
from pathlib import Path
from datetime import datetime

def inspect_database(db_path):
    """Inspect database contents and data quality"""
    print(f"Inspecting database: {db_path}")
    print("=" * 60)

    if not os.path.exists(db_path):
        print(f"Database file not found: {db_path}")
        return

    # Connect to database
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    try:
        # Get all tables
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
        tables = cursor.fetchall()

        print(f"Found {len(tables)} tables:")
        for table in tables:
            print(f"  - {table[0]}")

        print("\n" + "=" * 60)

        # Inspect restaurants table
        if any(table[0] == 'restaurants' for table in tables):
            inspect_restaurants_table(cursor)
        else:
            print("No restaurants table found")

        # Check for enrichment tables
        enrichment_tables = ['concept_classifications', 'population_data', 'square_footage_data']
        for table_name in enrichment_tables:
            if any(t[0] == table_name for t in tables):
                inspect_enrichment_table(cursor, table_name)

    except Exception as e:
        print(f"Error inspecting database: {e}")
    finally:
        conn.close()

def inspect_restaurants_table(cursor):
    """Inspect the main restaurants table"""
    print("RESTAURANTS TABLE INSPECTION")
    print("-" * 40)

    # Get row count
    cursor.execute("SELECT COUNT(*) FROM restaurants")
    count = cursor.fetchone()[0]
    print(f"Total restaurants: {count:,}")

    if count == 0:
        print("No restaurant data found")
        return

    # Show sample records
    print("\nSample restaurant records:")
    cursor.execute("""
        SELECT id, location_name, location_address, location_city, location_state,
               location_zip, total_receipts, tabc_permit_number
        FROM restaurants
        LIMIT 5
    """)

    sample_records = cursor.fetchall()
    for i, record in enumerate(sample_records, 1):
        print(f"\n  Record {i}:")
        print(f"    Name: {record[1]}")
        print(f"    Address: {record[2]}, {record[3]}, {record[4]} {record[5]}")
        print(f"    Total Receipts: ${record[6]:,.2f}")
        print(f"    TABC Permit: {record[7]}")

    # Show data quality statistics
    print("\nData Quality Statistics:")

    # Check for missing location names
    cursor.execute("SELECT COUNT(*) FROM restaurants WHERE location_name IS NULL OR location_name = ''")
    missing_names = cursor.fetchone()[0]
    print(f"  - Missing location names: {missing_names}")

    # Check for missing addresses
    cursor.execute("SELECT COUNT(*) FROM restaurants WHERE location_address IS NULL OR location_address = ''")
    missing_addresses = cursor.fetchone()[0]
    print(f"  - Missing addresses: {missing_addresses}")

    # Check for zero receipts (might indicate inactive)
    cursor.execute("SELECT COUNT(*) FROM restaurants WHERE total_receipts = 0 OR total_receipts IS NULL")
    zero_receipts = cursor.fetchone()[0]
    print(f"  - Zero/NULL receipts: {zero_receipts}")

    # Show top cities by count
    print("\nTop 5 cities by restaurant count:")
    cursor.execute("""
        SELECT location_city, COUNT(*) as count
        FROM restaurants
        WHERE location_city IS NOT NULL AND location_city != ''
        GROUP BY location_city
        ORDER BY count DESC
        LIMIT 5
    """)

    cities = cursor.fetchall()
    for city, count in cities:
        print(f"  - {city}: {count} restaurants")

    # Show receipt statistics
    cursor.execute("""
        SELECT MIN(total_receipts) as min_receipts,
               MAX(total_receipts) as max_receipts,
               AVG(total_receipts) as avg_receipts
        FROM restaurants
        WHERE total_receipts > 0
    """)

    stats = cursor.fetchone()
    if stats[0] is not None:
        print("\nReceipt Statistics (for restaurants with > $0 receipts):")
        print(f"  - Smallest: ${stats[0]:,.2f}")
        print(f"  - Largest: ${stats[1]:,.2f}")
        print(f"  - Average: ${stats[2]:,.2f}")

def inspect_enrichment_table(cursor, table_name):
    """Inspect enrichment data tables"""
    print(f"\n{table_name.upper().replace('_', ' ')} TABLE")
    print("-" * 40)

    cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
    count = cursor.fetchone()[0]
    print(f"Total records: {count:,}")

    if count == 0:
        print("No enrichment data found")
        return

    # Show sample enrichment data
    cursor.execute(f"SELECT * FROM {table_name} LIMIT 3")
    columns = [desc[0] for desc in cursor.description]
    rows = cursor.fetchall()

    print("\nSample enrichment records:")
    for i, row in enumerate(rows, 1):
        print(f"\n  Record {i}:")
        for col, value in zip(columns, row):
            print(f"    {col}: {value}")

if __name__ == "__main__":
    # Check multiple possible database locations
    possible_dbs = [
        'tabc_restaurants.db',
        'src/tabc_restaurants.db',
        'dev_tabc_restaurants.db',
        'src/dev_tabc_restaurants.db',
        'test_restaurants.db'
    ]

    found_dbs = []
    for db_path in possible_dbs:
        if os.path.exists(db_path):
            found_dbs.append(db_path)

    if not found_dbs:
        print("No database files found in expected locations")
        print("Expected locations:")
        for db_path in possible_dbs:
            print(f"  - {db_path}")
    else:
        print(f"Found {len(found_dbs)} database file(s)")
        for db_path in found_dbs:
            inspect_database(db_path)
            print("\n" + "=" * 80 + "\n")
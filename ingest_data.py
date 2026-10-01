import os
import time
from dotenv import load_dotenv
import snowflake.connector

load_dotenv(r"D:\Yelp_Analysis\.env")

def main():
    print("=" * 70)
    print(" Snowflake Ingestion: Loading Staged Yelp Datasets into Tables")
    print("=" * 70)

    conn = snowflake.connector.connect(
        account=os.getenv("SNOWFLAKE_ACCOUNT"),
        user=os.getenv("SNOWFLAKE_USER"),
        password=os.getenv("SNOWFLAKE_PASSWORD"),
        role=os.getenv("SNOWFLAKE_ROLE"),
        warehouse=os.getenv("SNOWFLAKE_WAREHOUSE", "YELP_WH"),
        database=os.getenv("SNOWFLAKE_DATABASE", "YELP_DB"),
        schema=os.getenv("SNOWFLAKE_SCHEMA", "RAW"),
    )
    cur = conn.cursor()

    try:
        # 1. Prepare environment & scale warehouse temporarily for fast bulk load
        print("1. Preparing warehouse and database context...")
        cur.execute("USE WAREHOUSE YELP_WH")
        cur.execute("USE DATABASE YELP_DB")
        cur.execute("USE SCHEMA RAW")
        
        # Scale to SMALL for parallel thread loading, will revert back to XSMALL
        cur.execute("ALTER WAREHOUSE YELP_WH SET WAREHOUSE_SIZE = 'SMALL' AUTO_SUSPEND = 60")

        # 2. Ingest Business Dataset
        print("\n2. Creating 'yelp_business' table and loading data...")
        cur.execute("CREATE OR REPLACE TABLE yelp_business (business_text VARIANT)")
        
        t0 = time.time()
        cur.execute("""
            COPY INTO yelp_business
            FROM @YELP_STAGE/business/
            FILE_FORMAT = (TYPE = 'JSON')
        """)
        biz_load_res = cur.fetchall()
        biz_elapsed = time.time() - t0
        print(f"   Loaded business dataset in {biz_elapsed:.2f} seconds.")
        
        cur.execute("SELECT COUNT(*) FROM yelp_business")
        biz_count = cur.fetchone()[0]
        print(f"   Total Business Records: {biz_count:,}")

        # 3. Ingest Reviews Dataset (all 10 partitions concurrently)
        print("\n3. Creating 'yelp_reviews' table and loading all 10 partition files...")
        cur.execute("CREATE OR REPLACE TABLE yelp_reviews (review_text VARIANT)")

        t1 = time.time()
        cur.execute("""
            COPY INTO yelp_reviews
            FROM @YELP_STAGE/reviews/
            FILE_FORMAT = (TYPE = 'JSON')
        """)
        rev_load_res = cur.fetchall()
        rev_elapsed = time.time() - t1
        print(f"   Loaded reviews dataset in {rev_elapsed:.2f} seconds ({rev_elapsed / 60:.2f} mins).")

        cur.execute("SELECT COUNT(*) FROM yelp_reviews")
        rev_count = cur.fetchone()[0]
        print(f"   Total Review Records: {rev_count:,}")

        # 4. Preview sample record from both tables
        print("\n4. Sample Record from 'yelp_business':")
        cur.execute("SELECT business_text FROM yelp_business LIMIT 1")
        print("  ", cur.fetchone()[0])

        print("\n5. Sample Record from 'yelp_reviews':")
        cur.execute("SELECT review_text FROM yelp_reviews LIMIT 1")
        print("  ", cur.fetchone()[0])

        # 5. Revert warehouse back to XSMALL to minimize credit usage
        cur.execute("ALTER WAREHOUSE YELP_WH SET WAREHOUSE_SIZE = 'XSMALL'")
        print("\nWarehouse resized back to XSMALL to save credits.")

        print("\n" + "=" * 70)
        print(f" SUCCESS: Ingested {biz_count:,} businesses and {rev_count:,} reviews!")
        print("=" * 70)

    except Exception as e:
        print(f"\n[ERROR] Ingestion failed: {e}")
        raise
    finally:
        cur.close()
        conn.close()

if __name__ == "__main__":
    main()

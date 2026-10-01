import os
import time
from dotenv import load_dotenv
import snowflake.connector

load_dotenv(r"D:\Yelp_Analysis\.env")

def main():
    print("=" * 70, flush=True)
    print(" Snowflake Transformations: Sentiment UDF & Relational Modeling", flush=True)
    print("=" * 70, flush=True)

    conn = snowflake.connector.connect(
        account=os.getenv("SNOWFLAKE_ACCOUNT"),
        user=os.getenv("SNOWFLAKE_USER"),
        password=os.getenv("SNOWFLAKE_PASSWORD"),
        role=os.getenv("SNOWFLAKE_ROLE"),
        warehouse=os.getenv("SNOWFLAKE_WAREHOUSE", "YELP_WH"),
        database=os.getenv("SNOWFLAKE_DATABASE", "YELP_DB"),
    )
    cur = conn.cursor()

    try:
        cur.execute("USE WAREHOUSE YELP_WH")
        cur.execute("USE DATABASE YELP_DB")

        # 1. Create ANALYTICS schema
        print("1. Creating 'ANALYTICS' schema...", flush=True)
        cur.execute("CREATE SCHEMA IF NOT EXISTS ANALYTICS")
        cur.execute("USE SCHEMA ANALYTICS")

        # 2. Deploy Python UDF for Sentiment Analysis (zero indentation for python body)
        print("\n2. Deploying Python Sentiment Analysis UDF ('analyze_sentiment')...", flush=True)
        udf_sql = """
CREATE OR REPLACE FUNCTION analyze_sentiment(text STRING)
RETURNS STRING
LANGUAGE PYTHON
RUNTIME_VERSION = '3.10'
PACKAGES = ('textblob')
HANDLER = 'sentiment_analyzer'
AS $$
from textblob import TextBlob

def sentiment_analyzer(text):
    if not text:
        return 'Neutral'
    analysis = TextBlob(text)
    if analysis.sentiment.polarity > 0:
        return 'Positive'
    elif analysis.sentiment.polarity == 0:
        return 'Neutral'
    else:
        return 'Negative'
$$;
"""
        cur.execute(udf_sql)
        print("   UDF created successfully!", flush=True)

        # 3. Test the UDF
        print("\n3. Testing UDF with sample inputs:", flush=True)
        test_query = """
            SELECT 
                column1 AS sample_text,
                analyze_sentiment(column1) AS predicted_sentiment
            FROM VALUES 
                ('The food and ambiance was absolutely amazing, best dining experience ever!'),
                ('The burger was okay, nothing special but edible.'),
                ('Terrible service, cold food, and rude staff. Never coming back!')
        """
        cur.execute(test_query)
        for row in cur.fetchall():
            print(f"   [{row[1]:<8}] \"{row[0]}\"", flush=True)

        # 4. Create Structured DIM_BUSINESS Table
        print("\n4. Creating structured 'DIM_BUSINESS' table from raw JSON...", flush=True)
        t0 = time.time()
        cur.execute("""
            CREATE OR REPLACE TABLE DIM_BUSINESS AS
            SELECT
                business_text:business_id::STRING       AS business_id,
                business_text:name::STRING              AS business_name,
                business_text:address::STRING           AS address,
                business_text:city::STRING              AS city,
                business_text:state::STRING             AS state,
                business_text:postal_code::STRING       AS postal_code,
                business_text:latitude::FLOAT           AS latitude,
                business_text:longitude::FLOAT          AS longitude,
                business_text:stars::FLOAT              AS stars,
                business_text:review_count::INT         AS review_count,
                business_text:is_open::INT              AS is_open,
                business_text:categories::STRING        AS categories
            FROM YELP_DB.RAW.YELP_BUSINESS
        """)
        print(f"   DIM_BUSINESS created in {time.time()-t0:.2f}s.", flush=True)

        cur.execute("SELECT COUNT(*) FROM DIM_BUSINESS")
        print(f"   DIM_BUSINESS rows: {cur.fetchone()[0]:,}", flush=True)

        # 5. Create Structured VW_REVIEWS View
        print("\n5. Creating structured 'VW_REVIEWS' view from raw JSON...", flush=True)
        cur.execute("""
            CREATE OR REPLACE VIEW VW_REVIEWS AS
            SELECT
                review_text:review_id::STRING           AS review_id,
                review_text:business_id::STRING         AS business_id,
                review_text:user_id::STRING             AS user_id,
                review_text:stars::INT                  AS stars,
                review_text:date::TIMESTAMP_NTZ         AS review_date,
                review_text:text::STRING                AS review_text,
                review_text:useful::INT                 AS useful_votes,
                review_text:funny::INT                  AS funny_votes,
                review_text:cool::INT                   AS cool_votes
            FROM YELP_DB.RAW.YELP_REVIEWS
        """)
        print("   VW_REVIEWS view created successfully!", flush=True)

        # 6. Create Materialized Analyzed Sample for Business Intelligence (100,000 reviews)
        print("\n6. Creating 'FACT_REVIEWS_ENRICHED' with Sentiment Analysis for Analytics...", flush=True)
        print("   (Applying analyze_sentiment() to 100,000 recent reviews for fast querying & Power BI)...", flush=True)
        
        # Scale warehouse to SMALL for fast UDF processing
        cur.execute("ALTER WAREHOUSE YELP_WH SET WAREHOUSE_SIZE = 'SMALL'")
        
        t1 = time.time()
        cur.execute("""
            CREATE OR REPLACE TABLE FACT_REVIEWS_ENRICHED AS
            SELECT
                r.review_id,
                r.business_id,
                r.user_id,
                r.stars,
                r.review_date,
                r.useful_votes,
                r.funny_votes,
                r.cool_votes,
                r.review_text,
                analyze_sentiment(r.review_text) AS sentiment
            FROM VW_REVIEWS r
            QUALIFY ROW_NUMBER() OVER (ORDER BY r.review_date DESC) <= 100000
        """)
        print(f"   FACT_REVIEWS_ENRICHED created in {time.time()-t1:.2f}s.", flush=True)

        # Scale warehouse back to XSMALL
        cur.execute("ALTER WAREHOUSE YELP_WH SET WAREHOUSE_SIZE = 'XSMALL'")

        cur.execute("""
            SELECT sentiment, COUNT(*), ROUND(COUNT(*)*100.0 / SUM(COUNT(*)) OVER(), 2) AS pct
            FROM FACT_REVIEWS_ENRICHED
            GROUP BY sentiment
            ORDER BY COUNT(*) DESC
        """)
        print("\nSentiment Distribution on Enriched Reviews:", flush=True)
        print("-" * 45, flush=True)
        print(f"{'Sentiment':<12} | {'Count':<10} | {'Percentage':<10}", flush=True)
        print("-" * 45, flush=True)
        for row in cur.fetchall():
            print(f"{row[0]:<12} | {row[1]:<10,} | {row[2]:.2f}%", flush=True)
        print("-" * 45, flush=True)

        print("\n" + "=" * 70, flush=True)
        print(" SUCCESS: UDF deployed and analytics tables created successfully!", flush=True)
        print("=" * 70, flush=True)

    except Exception as e:
        print(f"\n[ERROR]: {e}", flush=True)
        raise
    finally:
        cur.close()
        conn.close()

if __name__ == "__main__":
    main()

import os
import time
from dotenv import load_dotenv
import snowflake.connector

load_dotenv(r"D:\Yelp_Analysis\.env")

def print_table(title, headers, rows):
    print("\n" + "=" * 80)
    print(f" {title}")
    print("=" * 80)
    col_widths = [max(len(str(h)), max((len(str(r[i])) for r in rows), default=0)) + 3 for i, h in enumerate(headers)]
    
    header_str = "".join(f"{str(h):<{col_widths[i]}}" for i, h in enumerate(headers))
    print(header_str)
    print("-" * len(header_str))
    
    for row in rows:
        row_str = "".join(f"{str(r):<{col_widths[i]}}" for i, r in enumerate(row))
        print(row_str)
    print("-" * len(header_str))

def main():
    conn = snowflake.connector.connect(
        account=os.getenv("SNOWFLAKE_ACCOUNT"),
        user=os.getenv("SNOWFLAKE_USER"),
        password=os.getenv("SNOWFLAKE_PASSWORD"),
        role=os.getenv("SNOWFLAKE_ROLE"),
        warehouse=os.getenv("SNOWFLAKE_WAREHOUSE", "YELP_WH"),
        database=os.getenv("SNOWFLAKE_DATABASE", "YELP_DB"),
        schema="ANALYTICS",
    )
    cur = conn.cursor()

    try:
        cur.execute("USE WAREHOUSE YELP_WH")
        cur.execute("USE DATABASE YELP_DB")
        cur.execute("USE SCHEMA ANALYTICS")

        # -------------------------------------------------------------
        # Query 1: Sentiment vs Star Rating Correlation
        # -------------------------------------------------------------
        q1 = """
        SELECT 
            stars,
            COUNT(*) AS total_reviews,
            COUNT(CASE WHEN sentiment = 'Positive' THEN 1 END) AS positive_reviews,
            ROUND(COUNT(CASE WHEN sentiment = 'Positive' THEN 1 END) * 100.0 / COUNT(*), 1) AS pct_positive,
            COUNT(CASE WHEN sentiment = 'Negative' THEN 1 END) AS negative_reviews,
            ROUND(COUNT(CASE WHEN sentiment = 'Negative' THEN 1 END) * 100.0 / COUNT(*), 1) AS pct_negative,
            COUNT(CASE WHEN sentiment = 'Neutral' THEN 1 END) AS neutral_reviews
        FROM FACT_REVIEWS_ENRICHED
        GROUP BY stars
        ORDER BY stars DESC;
        """
        cur.execute(q1)
        headers1 = ["Stars", "Total Reviews", "Positive", "% Pos", "Negative", "% Neg", "Neutral"]
        print_table("1. Sentiment vs. Star Rating Correlation", headers1, cur.fetchall())

        # -------------------------------------------------------------
        # Query 2: Rating vs Sentiment Mismatch (Sarcasm or Rating Errors)
        # -------------------------------------------------------------
        q2 = """
        SELECT 
            review_id,
            stars,
            sentiment,
            LEFT(review_text, 100) || '...' AS sample_review_snippet
        FROM FACT_REVIEWS_ENRICHED
        WHERE (stars = 1 AND sentiment = 'Positive')
           OR (stars = 5 AND sentiment = 'Negative')
        LIMIT 5;
        """
        cur.execute(q2)
        headers2 = ["Review ID", "Stars", "Sentiment", "Review Snippet"]
        print_table("2. Rating Mismatches (Sarcasm or Inconsistent Reviews)", headers2, cur.fetchall())

        # -------------------------------------------------------------
        # Query 3: Top Cities by Review Volume and Positive Sentiment Rate
        # -------------------------------------------------------------
        q3 = """
        SELECT 
            b.city,
            b.state,
            COUNT(*) AS total_reviews,
            ROUND(AVG(r.stars), 2) AS avg_stars,
            ROUND(COUNT(CASE WHEN r.sentiment = 'Positive' THEN 1 END) * 100.0 / COUNT(*), 1) AS pct_positive_sentiment,
            ROUND(COUNT(CASE WHEN r.sentiment = 'Negative' THEN 1 END) * 100.0 / COUNT(*), 1) AS pct_negative_sentiment
        FROM FACT_REVIEWS_ENRICHED r
        JOIN DIM_BUSINESS b ON r.business_id = b.business_id
        GROUP BY b.city, b.state
        HAVING COUNT(*) >= 1000
        ORDER BY total_reviews DESC
        LIMIT 10;
        """
        cur.execute(q3)
        headers3 = ["City", "State", "Reviews", "Avg Stars", "% Positive", "% Negative"]
        print_table("3. Geographical Benchmark: Top 10 Cities", headers3, cur.fetchall())

        # -------------------------------------------------------------
        # Query 4: Most Reviewed Businesses and Customer Sentiment
        # -------------------------------------------------------------
        q4 = """
        SELECT 
            b.business_name,
            b.city,
            b.stars AS overall_rating,
            COUNT(*) AS recent_reviews,
            ROUND(COUNT(CASE WHEN r.sentiment = 'Positive' THEN 1 END) * 100.0 / COUNT(*), 1) AS pct_positive,
            ROUND(COUNT(CASE WHEN r.sentiment = 'Negative' THEN 1 END) * 100.0 / COUNT(*), 1) AS pct_negative
        FROM FACT_REVIEWS_ENRICHED r
        JOIN DIM_BUSINESS b ON r.business_id = b.business_id
        GROUP BY b.business_name, b.city, b.stars
        HAVING COUNT(*) >= 50
        ORDER BY recent_reviews DESC
        LIMIT 10;
        """
        cur.execute(q4)
        headers4 = ["Business Name", "City", "Rating", "Reviews", "% Positive", "% Negative"]
        print_table("4. Top Reviewed Businesses Sentiment Profile", headers4, cur.fetchall())

        # -------------------------------------------------------------
        # Step 5: Create Direct Power BI View
        # -------------------------------------------------------------
        print("\n5. Creating 'VW_POWERBI_YELP_DASHBOARD' in Snowflake for Power BI...")
        cur.execute("""
            CREATE OR REPLACE VIEW VW_POWERBI_YELP_DASHBOARD AS
            SELECT
                r.review_id,
                r.review_date,
                DATE_TRUNC('month', r.review_date) AS review_month,
                r.stars AS review_stars,
                r.sentiment AS review_sentiment,
                r.useful_votes,
                r.funny_votes,
                r.cool_votes,
                b.business_id,
                b.business_name,
                b.city,
                b.state,
                b.postal_code,
                b.latitude,
                b.longitude,
                b.stars AS business_overall_stars,
                b.review_count AS business_total_reviews,
                b.is_open,
                b.categories
            FROM FACT_REVIEWS_ENRICHED r
            JOIN DIM_BUSINESS b ON r.business_id = b.business_id
        """)
        print("   VW_POWERBI_YELP_DASHBOARD created successfully!")

        cur.execute("SELECT COUNT(*) FROM VW_POWERBI_YELP_DASHBOARD")
        print(f"   Total rows in Power BI View: {cur.fetchone()[0]:,}")

    except Exception as e:
        print(f"\n[ERROR]: {e}")
        raise
    finally:
        cur.close()
        conn.close()

if __name__ == "__main__":
    main()

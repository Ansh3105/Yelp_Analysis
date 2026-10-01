-- =====================================================================================
-- YELP BUSINESS REVIEWS & SENTIMENT ANALYTICS - SNOWFLAKE SQL QUERIES
-- =====================================================================================
-- Database: YELP_DB
-- Schema:   ANALYTICS
-- Warehouse: YELP_WH
-- =====================================================================================

-- 0. Set Session Context
USE WAREHOUSE YELP_WH;
USE DATABASE YELP_DB;
USE SCHEMA ANALYTICS;


-- =====================================================================================
-- QUERY 1: Sentiment vs. Star Rating Correlation
-- Description: Analyzes how user star ratings (1-5) correlate with NLP sentiment labels.
-- =====================================================================================
SELECT 
    stars,
    COUNT(*) AS total_reviews,
    COUNT(CASE WHEN sentiment = 'Positive' THEN 1 END) AS positive_reviews,
    ROUND(COUNT(CASE WHEN sentiment = 'Positive' THEN 1 END) * 100.0 / COUNT(*), 1) AS pct_positive,
    COUNT(CASE WHEN sentiment = 'Negative' THEN 1 END) AS negative_reviews,
    ROUND(COUNT(CASE WHEN sentiment = 'Negative' THEN 1 END) * 100.0 / COUNT(*), 1) AS pct_negative,
    COUNT(CASE WHEN sentiment = 'Neutral' THEN 1 END) AS neutral_reviews,
    ROUND(COUNT(CASE WHEN sentiment = 'Neutral' THEN 1 END) * 100.0 / COUNT(*), 1) AS pct_neutral
FROM FACT_REVIEWS_ENRICHED
GROUP BY stars
ORDER BY stars DESC;


-- =====================================================================================
-- QUERY 2: Rating vs. Sentiment Mismatch (Detecting Sarcasm & Rating Errors)
-- Description: Identifies 1-star reviews with positive wording (sarcasm or praise for
--              servers despite bad management) and 5-star reviews with negative remarks.
-- =====================================================================================
SELECT 
    review_id,
    stars,
    sentiment,
    useful_votes,
    review_date,
    review_text
FROM FACT_REVIEWS_ENRICHED
WHERE (stars = 1 AND sentiment = 'Positive')
   OR (stars = 5 AND sentiment = 'Negative')
ORDER BY useful_votes DESC
LIMIT 20;


-- =====================================================================================
-- QUERY 3: Geographical Benchmark (Top 10 Most Reviewed Cities)
-- Description: Ranks cities by review volume, average stars, and positive sentiment rate.
-- =====================================================================================
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


-- =====================================================================================
-- QUERY 4: Top Reviewed Businesses Sentiment Profile
-- Description: Identifies the most reviewed businesses and their customer sentiment mix.
-- =====================================================================================
SELECT 
    b.business_name,
    b.city,
    b.state,
    b.stars AS overall_rating,
    COUNT(*) AS recent_reviews,
    ROUND(COUNT(CASE WHEN r.sentiment = 'Positive' THEN 1 END) * 100.0 / COUNT(*), 1) AS pct_positive,
    ROUND(COUNT(CASE WHEN r.sentiment = 'Negative' THEN 1 END) * 100.0 / COUNT(*), 1) AS pct_negative
FROM FACT_REVIEWS_ENRICHED r
JOIN DIM_BUSINESS b ON r.business_id = b.business_id
GROUP BY b.business_name, b.city, b.state, b.stars
HAVING COUNT(*) >= 50
ORDER BY recent_reviews DESC
LIMIT 15;


-- =====================================================================================
-- QUERY 5: Monthly Sentiment Trend Over Time
-- Description: Analyzes monthly customer sentiment trajectory to spot seasonal trends.
-- =====================================================================================
SELECT 
    DATE_TRUNC('month', review_date) AS review_month,
    COUNT(*) AS total_reviews,
    COUNT(CASE WHEN sentiment = 'Positive' THEN 1 END) AS positive_count,
    COUNT(CASE WHEN sentiment = 'Negative' THEN 1 END) AS negative_count,
    ROUND(AVG(stars), 2) AS avg_monthly_stars
FROM FACT_REVIEWS_ENRICHED
GROUP BY DATE_TRUNC('month', review_date)
ORDER BY review_month DESC
LIMIT 24;


-- =====================================================================================
-- QUERY 6: Power BI Unified Reporting View
-- Description: The exact definition of the denormalized view used by Power BI Desktop.
-- =====================================================================================
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
LIMIT 100;

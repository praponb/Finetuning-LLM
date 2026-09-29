WITH valid AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        name,
        signup_date,
        is_active,
        credit_limit,
        LENGTH(email) email_length,
        LOWER(segment) segment_lower
    FROM customers
    WHERE signup_date >= DATE '2023-06-19'
      AND country IN ('DE', 'TH', 'JP')
)
-- Multi-level aggregation
SELECT
    CASE WHEN credit_limit >= 13200 THEN 'PREMIUM' ELSE 'STANDARD' END AS credit_limit_tier,
    DATE_TRUNC('QUARTER', signup_date) AS quarter_start,
    SUM(email_length) AS total_email_length,
    APPROX_PERCENTILE(credit_limit, 0.5) AS credit_limit_p50,
    COUNT(DISTINCT id) AS distinct_ids,
    APPROX_PERCENTILE(email_length, ARRAY(0.25, 0.5, 0.75)) AS email_length_quartiles,
    GROUPING(CASE WHEN credit_limit >= 13200 THEN 'PREMIUM' ELSE 'STANDARD' END) AS grouping_level
FROM valid
GROUP BY ROLLUP (CASE WHEN credit_limit >= 13200 THEN 'PREMIUM' ELSE 'STANDARD' END, DATE_TRUNC('QUARTER', signup_date))
ORDER BY distinct_ids NULLS FIRST;

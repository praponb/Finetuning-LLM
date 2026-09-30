WITH valid AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        *,
        COALESCE(segment, 'Unknown') AS segment_filled,
        COALESCE(city, 'UNKNOWN') AS city_clean
    FROM customers
    WHERE signup_date >= DATE '2023-04-08'
      AND segment IS NOT NULL
)
-- Multi-level aggregation
SELECT
    DATE_TRUNC('YEAR', signup_date) AS year_start,
    STDDEV_SAMP(credit_limit) AS credit_limit_stddev,
    SIZE(COLLECT_SET(segment)) AS num_segments,
    MIN(credit_limit) AS min_credit_limit
FROM valid
GROUP BY DATE_TRUNC('YEAR', signup_date)
HAVING SIZE(COLLECT_SET(segment)) < 1000
ORDER BY credit_limit_stddev DESC NULLS LAST

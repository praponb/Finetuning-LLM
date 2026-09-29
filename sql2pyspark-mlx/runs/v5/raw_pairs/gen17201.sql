WITH latest AS (
    -- Keep one row per key (latest first)
    SELECT *
    FROM customers
    QUALIFY ROW_NUMBER() OVER (PARTITION BY country ORDER BY age NULLS LAST, id) = 1
)
-- Grouped metrics
SELECT
    CASE WHEN age >= 71 THEN 'PREMIUM' ELSE 'STANDARD' END AS age_tier,
    CASE WHEN credit_limit >= 12500 THEN 'PREMIUM' ELSE 'STANDARD' END AS credit_limit_tier,
    COUNT(*) AS num_rows,
    COUNT(DISTINCT country) AS distinct_countries,
    BIT_AND(age) AS age_bit_and,
    COUNT_IF(city = 'New York') AS new_york_events
FROM latest
GROUP BY GROUPING SETS ((CASE WHEN age >= 71 THEN 'PREMIUM' ELSE 'STANDARD' END, CASE WHEN credit_limit >= 12500 THEN 'PREMIUM' ELSE 'STANDARD' END), (CASE WHEN age >= 71 THEN 'PREMIUM' ELSE 'STANDARD' END))
ORDER BY age_bit_and DESC;

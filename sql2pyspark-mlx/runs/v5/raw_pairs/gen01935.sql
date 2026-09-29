WITH src AS (
    SELECT *
    FROM customers
    WHERE LENGTH(name) > 11 OR email IS NOT NULL
),
customers_stats AS (
    SELECT segment, COUNT(DISTINCT age) AS unique_segments, SUM(CASE WHEN segment = 'Consumer' THEN credit_limit ELSE 0 END) AS consumer_credit_limit
    FROM src
    GROUP BY segment
)
SELECT segment, unique_segments, consumer_credit_limit
FROM customers_stats
ORDER BY segment DESC
LIMIT 1;

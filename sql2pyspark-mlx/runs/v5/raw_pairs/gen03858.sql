WITH filtered_customers AS (
    SELECT *
    FROM customers
    WHERE name LIKE 'b%'
),
summary AS (
    SELECT
        city,
        segment,
        ROUND(STDDEV(age), 2) AS stddev_age,
        ROUND((100 * SUM(CASE WHEN segment = 'Consumer' THEN 1 ELSE 0 END)) / COUNT(*), 1) AS pct_consumer
    FROM filtered_customers
    GROUP BY city, segment
)
SELECT *
FROM summary
WHERE stddev_age < 47;

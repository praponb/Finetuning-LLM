WITH base AS (
    SELECT *
    FROM suppliers
    WHERE country = 'US' AND (country = 'US' OR supplier_name LIKE 'Gl%')
),
rating_bucket_summary AS (
    SELECT CASE WHEN rating >= 4.2 THEN 'high' WHEN rating >= 1.6 THEN 'mid' ELSE 'low' END AS rating_bucket, ROUND((100 * SUM(CASE WHEN city = 'Berlin' THEN 1 ELSE 0 END)) / COUNT(*), 1) AS pct_berlin
    FROM base
    GROUP BY CASE WHEN rating >= 4.2 THEN 'high' WHEN rating >= 1.6 THEN 'mid' ELSE 'low' END
)
SELECT rating_bucket, pct_berlin
FROM rating_bucket_summary
WHERE pct_berlin < 40

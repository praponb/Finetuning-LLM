WITH Base AS (
    SELECT *
    FROM customers
    WHERE email LIKE '%@outlook.com'
),
customers_stats AS (
    SELECT CASE WHEN credit_limit >= 13600 THEN 'high' WHEN credit_limit >= 8200 THEN 'mid' ELSE 'low' END AS credit_limit_bucket, SUM(CASE WHEN country = 'DE' THEN 1 ELSE 0 END) AS de_count
    FROM Base
    GROUP BY CASE WHEN credit_limit >= 13600 THEN 'high' WHEN credit_limit >= 8200 THEN 'mid' ELSE 'low' END
)
SELECT *
FROM customers_stats
ORDER BY de_count

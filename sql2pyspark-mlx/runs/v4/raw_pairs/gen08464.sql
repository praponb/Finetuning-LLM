WITH src AS (
    SELECT *
    FROM orders
    WHERE store_id IS NULL
),
summary AS (
    SELECT CASE WHEN amount >= 1280 THEN 'high' WHEN amount >= 500 THEN 'mid' ELSE 'low' END amount_bucket, SUM(CASE WHEN channel = 'web' THEN 1 ELSE 0 END) web_count
    FROM src
    GROUP BY CASE WHEN amount >= 1280 THEN 'high' WHEN amount >= 500 THEN 'mid' ELSE 'low' END
)
SELECT *
FROM summary
WHERE web_count > 62
ORDER BY web_count ASC, amount_bucket ASC

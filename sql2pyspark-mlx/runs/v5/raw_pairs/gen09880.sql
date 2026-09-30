WITH recent_order_items AS (
    SELECT *
    FROM order_items
    WHERE unit_price <= 220 AND unit_price BETWEEN 160 AND 370
),
unit_price_bucket_summary AS (
    SELECT CASE WHEN unit_price >= 430 THEN 'high' WHEN unit_price >= 80 THEN 'mid' ELSE 'low' END AS unit_price_bucket, ROUND(STDDEV(unit_price), 2) AS stddev_unit_price
    FROM recent_order_items
    GROUP BY CASE WHEN unit_price >= 430 THEN 'high' WHEN unit_price >= 80 THEN 'mid' ELSE 'low' END
)
SELECT unit_price_bucket, stddev_unit_price
FROM unit_price_bucket_summary
ORDER BY unit_price_bucket DESC, stddev_unit_price DESC

WITH src AS (
    SELECT *
    FROM order_items
    WHERE NOT (quantity <= 6 AND order_id = 27)
),
agg AS (
    SELECT CASE WHEN unit_price >= 390 THEN 'high' WHEN unit_price >= 170 THEN 'mid' ELSE 'low' END AS unit_price_bucket, ROUND(AVG(unit_price), 2) AS avg_unit_price
    FROM src
    GROUP BY CASE WHEN unit_price >= 390 THEN 'high' WHEN unit_price >= 170 THEN 'mid' ELSE 'low' END
)
SELECT unit_price_bucket, avg_unit_price
FROM agg
ORDER BY unit_price_bucket DESC;

WITH recent_order_items AS (
    SELECT *
    FROM order_items
    WHERE order_id IN (9, 14, 18, 33) AND order_id IN (17, 23)
),
UnitPriceBucketSummary AS (
    SELECT CASE WHEN unit_price >= 370 THEN 'high' WHEN unit_price >= 160 THEN 'mid' ELSE 'low' END AS unit_price_bucket, SUM(unit_price * quantity) AS revenue, MAX(quantity) - MIN(quantity) AS quantity_range
    FROM recent_order_items
    GROUP BY CASE WHEN unit_price >= 370 THEN 'high' WHEN unit_price >= 160 THEN 'mid' ELSE 'low' END
)
SELECT unit_price_bucket, revenue, quantity_range
FROM UnitPriceBucketSummary
WHERE quantity_range < 28;

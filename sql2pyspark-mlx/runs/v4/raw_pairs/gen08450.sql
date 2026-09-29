WITH filtered_order_items AS (
    SELECT *, ROUND(unit_price / quantity, 2) AS line_total
    FROM order_items
    WHERE quantity != 3
),
summary AS (
    SELECT CASE WHEN unit_price >= 320 THEN 'high' WHEN unit_price >= 50 THEN 'mid' ELSE 'low' END AS unit_price_bucket, COUNT(DISTINCT line_total) AS unique_orders, ROUND(STDDEV(quantity), 2) AS stddev_quantity
    FROM filtered_order_items
    GROUP BY CASE WHEN unit_price >= 320 THEN 'high' WHEN unit_price >= 50 THEN 'mid' ELSE 'low' END
)
SELECT *
FROM summary

WITH valid_orders AS (
    SELECT *
    FROM orders
    WHERE store_id IN (10, 25, 27, 30)
),
orders_stats AS (
    SELECT CASE WHEN discount >= 0.33 THEN 'high' WHEN discount >= 0.1 THEN 'mid' ELSE 'low' END AS discount_bucket, COUNT(DISTINCT shipping_fee) AS unique_statuss, MAX(order_date) AS latest_order_date
    FROM valid_orders
    GROUP BY CASE WHEN discount >= 0.33 THEN 'high' WHEN discount >= 0.1 THEN 'mid' ELSE 'low' END
)
SELECT *
FROM orders_stats;

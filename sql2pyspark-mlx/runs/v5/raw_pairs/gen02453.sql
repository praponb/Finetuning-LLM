WITH valid_orders AS (
    SELECT *, DAYOFWEEK(order_date) AS order_date_dayofweek
    FROM orders
    WHERE store_id IN (6, 20, 29, 36) AND order_date >= '2023-06-01' AND store_id IN (3, 12, 15, 38)
),
channel_summary AS (
    SELECT channel, CASE WHEN discount >= 0.42 THEN 'high' WHEN discount >= 0.2 THEN 'mid' ELSE 'low' END AS discount_bucket, ROUND(AVG(discount), 2) AS avg_discount
    FROM valid_orders
    GROUP BY channel, CASE WHEN discount >= 0.42 THEN 'high' WHEN discount >= 0.2 THEN 'mid' ELSE 'low' END
)
SELECT channel, discount_bucket, avg_discount
FROM channel_summary
ORDER BY avg_discount ASC;

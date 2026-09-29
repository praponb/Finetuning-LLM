WITH filtered_orders AS (
    SELECT *, LAST_DAY(order_date) AS order_date_month_end
    FROM orders
    WHERE amount BETWEEN 1080 AND 1460
),
orders_stats AS (
    SELECT CASE WHEN discount >= 0.35 THEN 'high' WHEN discount >= 0.16 THEN 'mid' ELSE 'low' END AS discount_bucket, ROUND(STDDEV(discount), 2) AS stddev_discount, COUNT(DISTINCT discount) AS unique_customers
    FROM filtered_orders
    GROUP BY CASE WHEN discount >= 0.35 THEN 'high' WHEN discount >= 0.16 THEN 'mid' ELSE 'low' END
)
SELECT discount_bucket, stddev_discount, unique_customers
FROM orders_stats
WHERE unique_customers >= 4
ORDER BY 2 DESC, unique_customers, discount_bucket
LIMIT 10;

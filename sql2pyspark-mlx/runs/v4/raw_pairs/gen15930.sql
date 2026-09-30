WITH customer_stats AS (
    SELECT customer_id, ROUND(STDDEV(shipping_fee), 2) AS stddev_shipping_fee, COUNT(DISTINCT amount) AS unique_stores
    FROM orders
    WHERE YEAR(order_date) >= 2025
    GROUP BY customer_id
)
SELECT
    c.email,
    c.city,
    cs.stddev_shipping_fee,
    cs.unique_stores
FROM customers c
LEFT JOIN customer_stats cs ON c.id = cs.customer_id
WHERE cs.stddev_shipping_fee > 52
ORDER BY c.email DESC, unique_stores;

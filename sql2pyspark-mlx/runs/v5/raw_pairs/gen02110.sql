WITH top_orders AS (
    SELECT *, RANK() OVER (PARTITION BY store_id ORDER BY order_date) AS rnk
    FROM orders
)
SELECT id, store_id, order_date
FROM top_orders
WHERE rnk <= 3;

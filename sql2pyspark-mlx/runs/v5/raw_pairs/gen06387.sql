WITH top_orders AS (
    SELECT
        id,
        customer_id,
        order_date,
        ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY order_date DESC, id) AS rnk
    FROM orders
)
SELECT id, customer_id, order_date
FROM top_orders
WHERE rnk <= 3
ORDER BY order_date DESC, customer_id DESC

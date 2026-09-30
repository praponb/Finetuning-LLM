WITH order_totals AS (
    SELECT order_id, MIN(amount) AS min_amount
    FROM payments
    GROUP BY order_id
)
SELECT o.customer_id, o.discount, ot.min_amount
FROM orders o
INNER JOIN order_totals ot ON o.id = ot.order_id
ORDER BY o.customer_id;

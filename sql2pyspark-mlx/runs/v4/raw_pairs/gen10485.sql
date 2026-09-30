WITH order_summary AS (
    SELECT order_id, ROUND(STDDEV(cost), 2) AS stddev_cost
    FROM shipments
    WHERE YEAR(delivered_date) = 2024
    GROUP BY order_id
)
SELECT
    o.id,
    o.order_date,
    o.amount,
    os.stddev_cost
FROM orders o
LEFT JOIN order_summary os ON o.id = os.order_id
WHERE os.stddev_cost > 40
ORDER BY o.amount, order_date DESC

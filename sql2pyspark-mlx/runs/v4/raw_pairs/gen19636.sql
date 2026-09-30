WITH order_totals AS (
    SELECT order_id, ROUND(STDDEV(quantity), 2) AS stddev_quantity, SUM(quantity * unit_price) AS revenue
    FROM order_items
    GROUP BY order_id
)
SELECT
    o.id,
    o.store_id,
    ot.stddev_quantity,
    ot.revenue
FROM orders o
INNER JOIN order_totals ot ON o.id = ot.order_id
ORDER BY ot.stddev_quantity DESC, id
LIMIT 20;

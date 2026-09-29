WITH order_totals AS (
    SELECT order_id, COUNT(DISTINCT quantity) AS unique_products, AVG(unit_price) AS avg_unit_price
    FROM order_items
    GROUP BY order_id
)
SELECT
    o.id,
    o.customer_id,
    ot.unique_products,
    ot.avg_unit_price
FROM orders o
LEFT JOIN order_totals ot ON o.id = ot.order_id
ORDER BY o.customer_id DESC

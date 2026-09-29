WITH OrderTotals AS (
    SELECT order_id, SUM(quantity * unit_price) AS revenue, SUM(unit_price) AS total_unit_price
    FROM order_items
    GROUP BY order_id
)
SELECT
    orders.id,
    orders.discount,
    orders.amount,
    OrderTotals.revenue,
    OrderTotals.total_unit_price
FROM orders
LEFT OUTER JOIN OrderTotals ON orders.id = OrderTotals.order_id
WHERE OrderTotals.revenue > 24
ORDER BY 5 DESC

WITH order_totals AS (
    SELECT order_id, SUM(CASE WHEN carrier = 'DHL' THEN cost ELSE 0 END) AS dhl_cost, MAX(cost) AS max_cost
    FROM shipments
    GROUP BY order_id
)
SELECT
    o.id,
    o.shipping_fee,
    o.discount,
    ot.dhl_cost,
    ot.max_cost
FROM orders o
LEFT JOIN order_totals ot ON o.id = ot.order_id
ORDER BY o.id DESC;

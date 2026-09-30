SELECT
    o.id,
    o.customer_id,
    t.max_cost,
    t.last_delivered_date
FROM orders o
INNER JOIN (SELECT order_id, MAX(cost) AS max_cost, MAX(delivered_date) AS last_delivered_date
    FROM shipments
    WHERE carrier IN ('Flash', 'FedEx')
    GROUP BY order_id) t ON o.id = t.order_id
WHERE t.max_cost > 56

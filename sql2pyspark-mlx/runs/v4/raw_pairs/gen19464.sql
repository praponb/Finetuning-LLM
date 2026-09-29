WITH order_summary AS (
    SELECT order_id, COUNT(DISTINCT cost) AS unique_statuss, ROUND(AVG(cost), 2) AS avg_cost
    FROM shipments
    GROUP BY order_id
)
SELECT
    o.id,
    o.shipping_fee,
    os.unique_statuss,
    os.avg_cost
FROM orders o
INNER JOIN order_summary os ON o.id = os.order_id;

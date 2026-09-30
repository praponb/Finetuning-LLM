WITH order_summary AS (
    SELECT order_id, MAX(shipped_date) AS latest_shipped_date, COUNT(cost) AS cost_count
    FROM shipments
    GROUP BY order_id
)
SELECT
    o.id,
    o.store_id,
    o.amount,
    os.latest_shipped_date,
    os.cost_count
FROM orders o
INNER JOIN order_summary os ON o.id = os.order_id
WHERE os.cost_count >= 5;

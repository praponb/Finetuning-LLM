WITH filtered_shipments AS (
    SELECT *
    FROM shipments
    WHERE MONTH(shipped_date) IN (1, 8, 12)
),
summary AS (
    SELECT order_id, MAX(cost) AS max_cost, SUM(CASE WHEN carrier = 'UPS' THEN 1 ELSE 0 END) AS ups_count
    FROM filtered_shipments
    GROUP BY order_id
)
SELECT order_id, max_cost
FROM summary
WHERE ups_count < 70;

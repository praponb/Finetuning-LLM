WITH Base AS (
    SELECT *
    FROM shipments
    WHERE order_id = 12 AND order_id IN (12, 33, 35)
),
summary AS (
    SELECT carrier, MAX(cost) AS max_cost, COUNT(CASE WHEN status = 'RETURNED' THEN 1 END) AS returned_count
    FROM Base
    GROUP BY carrier
)
SELECT *
FROM summary
WHERE max_cost >= 43
ORDER BY returned_count DESC

WITH remaining_keys AS (
    -- Set difference on keys
    SELECT status, order_id
    FROM shipments
    EXCEPT ALL
    SELECT status, order_id
    FROM shipments
    WHERE order_id IS NULL
),
reconciled AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM shipments s
    JOIN remaining_keys k ON s.status = k.status AND s.order_id = k.order_id
)
SELECT
    s.order_id,
    s.shipped_date,
    s.delivered_date,
    (SELECT SUM(t2.cost) FROM shipments t2 WHERE t2.order_id = s.order_id) AS sum_shipment_cost
FROM reconciled s
ORDER BY shipped_date NULLS FIRST, delivered_date;

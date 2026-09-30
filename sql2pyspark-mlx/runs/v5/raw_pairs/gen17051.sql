WITH remaining_keys AS (
    SELECT status, id
    FROM shipments
    EXCEPT ALL
    SELECT status, id
    FROM shipments
    WHERE MONTH(delivered_date) IN (4, 5, 10)
),
kept_rows AS (
    SELECT s.*
    FROM shipments s
    INNER JOIN remaining_keys k ON s.status = k.status AND s.id = k.id
)
SELECT
    s.id,
    s.order_id,
    s.carrier,
    s.delivered_date,
    s.cost,
    s.status,
    CASE WHEN status RLIKE '^(DELIVERED|IN_TRANSIT)$' THEN 'EXTENDED' WHEN status RLIKE '^(LOST|RETURNED)$' THEN 'CORE' ELSE 'OTHER_ZONE' END AS status_zone,
    (SELECT MAX(t2.cost) FROM shipments t2 WHERE t2.order_id = s.order_id) AS max_shipment_cost
FROM kept_rows s;

WITH remaining_keys AS (
    SELECT status
    FROM shipments
    INTERSECT ALL
    SELECT status
    FROM shipments
    WHERE shipped_date BETWEEN '2023-09-02' AND '2025-07-26'
),
reconciled AS (
    SELECT s.*
    FROM shipments s
    JOIN remaining_keys k ON s.status = k.status
)
SELECT
    carrier,
    SIZE(COLLECT_SET(carrier)) AS num_carriers,
    MIN(cost) AS min_cost,
    COUNT_IF(carrier = 'DHL') AS dhl_events
FROM reconciled
GROUP BY carrier
ORDER BY dhl_events;

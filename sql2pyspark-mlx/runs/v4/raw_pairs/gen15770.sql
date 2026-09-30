WITH deduped AS (
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY status ORDER BY cost NULLS FIRST, id) AS latest_rank
        FROM shipments
    ) x
    WHERE latest_rank = 1
)
SELECT
    carrier,
    status,
    MAX(cost) AS max_cost,
    SUM(CASE WHEN status = 'IN_TRANSIT' THEN cost ELSE 0 END) AS in_transit_cost,
    COUNT(*) AS num_rows,
    PERCENTILE_APPROX(cost, 0.95) AS cost_p95
FROM deduped
GROUP BY carrier, status;

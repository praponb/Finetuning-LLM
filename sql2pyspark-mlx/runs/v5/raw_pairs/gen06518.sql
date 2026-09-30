WITH kept_keys AS (
    -- Set difference on keys
    SELECT id, status
    FROM shipments
    EXCEPT DISTINCT
    SELECT id, status
    FROM shipments
    WHERE carrier IN ('DHL', 'Kerry', 'UPS')
),
reconciled AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM shipments s
    INNER JOIN kept_keys k ON s.id = k.id AND s.status = k.status
)
-- Aggregate with conditional metrics
SELECT
    status,
    PERCENTILE_APPROX(cost, 0.5) AS cost_p50,
    COUNT(*) FILTER (WHERE carrier = 'DHL') AS dhl_rows,
    AVG(cost) FILTER (WHERE status = 'RETURNED') AS returned_avg_cost
FROM reconciled
GROUP BY status;

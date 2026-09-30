WITH latest AS (
    -- Deduplicate with ROW_NUMBER
    SELECT *, ROW_NUMBER() OVER (PARTITION BY status, order_id ORDER BY shipped_date DESC NULLS FIRST, id) AS dedup_rank
    FROM shipments
    QUALIFY dedup_rank = 1
)
-- Multi-level aggregation
SELECT
    status,
    carrier,
    COUNT_IF(carrier = 'FedEx') AS fedex_events,
    PERCENTILE_APPROX(cost, ARRAY(0.25, 0.5, 0.75)) AS cost_quartiles
FROM latest
GROUP BY status, carrier
HAVING fedex_events > 1
ORDER BY fedex_events DESC

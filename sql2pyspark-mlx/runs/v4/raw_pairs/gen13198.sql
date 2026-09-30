WITH with_windows AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        PERCENT_RANK() OVER (PARTITION BY carrier ORDER BY cost NULLS FIRST) AS cost_percent_rank,
        PERCENT_RANK() OVER (ORDER BY cost) AS cost_percent_rank_2
    FROM shipments
)
SELECT
    id,
    order_id,
    shipped_date,
    delivered_date,
    status,
    cost_percent_rank_2
FROM with_windows
ORDER BY shipped_date DESC, status

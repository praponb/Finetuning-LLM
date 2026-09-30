WITH flattened AS (
    -- Unnest the array into one row per element
    SELECT id, EXPLODE_OUTER(tier_prices) AS val
    FROM price_tiers
)
-- Window metrics over several different partitions
SELECT
    id,
    val,
    CUME_DIST() OVER w_running AS val_cume_dist,
    RANK() OVER (ORDER BY val DESC) AS val_rank
FROM flattened
WINDOW w_running AS (ORDER BY val NULLS LAST)

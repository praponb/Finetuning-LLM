WITH lines AS (
    -- Unnest the array into one row per element
    SELECT id, val
    FROM price_tiers
    LATERAL VIEW OUTER EXPLODE(tier_limits) x AS val
)
-- Ranking and gap features
SELECT
    *,
    CUME_DIST() OVER (ORDER BY val DESC NULLS FIRST) AS val_cume_dist,
    RANK() OVER w_hist AS val_rank,
    DENSE_RANK() OVER w_hist AS val_dense_rank
FROM lines
WINDOW w_hist AS (ORDER BY val NULLS FIRST)

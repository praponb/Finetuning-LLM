WITH flattened AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        plan,
        effective_from,
        currency,
        val
    FROM price_tiers
    LATERAL VIEW OUTER EXPLODE(tier_limits) e AS val
)
-- Ranking and gap features
SELECT
    *,
    CUME_DIST() OVER (ORDER BY val) AS val_cume_dist,
    DENSE_RANK() OVER (ORDER BY val DESC NULLS FIRST) AS val_dense_rank,
    DENSE_RANK() OVER (ORDER BY val DESC) AS val_dense_rank_2
FROM flattened;

WITH flattened AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        plan,
        effective_from,
        currency,
        EXPLODE_OUTER(tier_prices) AS val
    FROM price_tiers
)
-- Ranking and gap features
SELECT
    plan,
    currency,
    val,
    RANK() OVER (PARTITION BY plan ORDER BY val DESC) AS val_rank,
    DENSE_RANK() OVER w_grp AS val_dense_rank,
    CUME_DIST() OVER w_grp AS val_cume_dist
FROM flattened
WINDOW w_grp AS (ORDER BY val NULLS LAST);

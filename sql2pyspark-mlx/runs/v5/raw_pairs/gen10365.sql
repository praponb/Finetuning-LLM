WITH lines AS (
    SELECT
        id,
        effective_from,
        EXPLODE(tier_limits) AS tier_value
    FROM price_tiers
),
filtered AS (
    SELECT *, ROUND(tier_value, 1) AS tier_value_rounded
    FROM lines
    WHERE tier_value BETWEEN 54 AND 62
),
scored AS (
    SELECT
        *,
        CUME_DIST() OVER (ORDER BY tier_value_rounded NULLS FIRST) AS tier_value_rounded_cume_dist,
        RANK() OVER (ORDER BY tier_value_rounded NULLS LAST) AS tier_value_rounded_rank,
        DENSE_RANK() OVER w_grp AS tier_value_dense_rank
    FROM filtered
    WINDOW w_grp AS (ORDER BY tier_value DESC)
),
kept_keys AS (
    SELECT id
    FROM scored
    INTERSECT ALL
    SELECT id
    FROM price_tiers
    WHERE plan = 'enterprise'
),
reconciled AS (
    SELECT s.*
    FROM scored s
    JOIN kept_keys k ON s.id = k.id
)
SELECT
    effective_from,
    tier_value_rounded,
    tier_value_rounded_cume_dist,
    tier_value_rounded_rank,
    tier_value_dense_rank
FROM reconciled
ORDER BY tier_value_rounded DESC

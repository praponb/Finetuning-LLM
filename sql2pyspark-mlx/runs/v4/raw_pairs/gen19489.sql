WITH exploded AS (
    SELECT
        id,
        effective_from,
        val
    FROM price_tiers
    LATERAL VIEW OUTER EXPLODE(tier_prices) li AS val
)
SELECT
    *,
    PERCENT_RANK() OVER (ORDER BY val DESC NULLS FIRST) AS val_percent_rank,
    CUME_DIST() OVER (ORDER BY val DESC) AS val_cume_dist,
    RANK() OVER (ORDER BY val DESC NULLS LAST) AS val_rank
FROM exploded

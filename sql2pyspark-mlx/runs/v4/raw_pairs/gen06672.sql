WITH lines AS (
    SELECT
        id,
        plan,
        currency,
        EXPLODE(tier_limits) AS tier_value
    FROM price_tiers
)
SELECT
    *,
    CUME_DIST() OVER (ORDER BY tier_value DESC) AS tier_value_cume_dist,
    PERCENT_RANK() OVER (ORDER BY tier_value DESC NULLS LAST) AS tier_value_percent_rank,
    DENSE_RANK() OVER (PARTITION BY currency ORDER BY tier_value DESC NULLS LAST) AS tier_value_dense_rank
FROM lines

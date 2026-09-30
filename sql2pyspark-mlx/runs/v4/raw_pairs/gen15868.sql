WITH exploded AS (
    SELECT
        id,
        plan,
        currency,
        EXPLODE_OUTER(tier_limits) AS tier_value
    FROM price_tiers
)
SELECT
    *,
    RANK() OVER (PARTITION BY plan ORDER BY tier_value DESC NULLS LAST) tier_value_rank,
    DENSE_RANK() OVER (PARTITION BY currency ORDER BY tier_value DESC NULLS FIRST) tier_value_dense_rank
FROM exploded

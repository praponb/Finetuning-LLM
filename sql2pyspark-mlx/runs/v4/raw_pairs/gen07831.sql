WITH exploded AS (
    SELECT
        id,
        effective_from,
        currency,
        tier_value
    FROM price_tiers
    LATERAL VIEW EXPLODE(tier_prices) t AS tier_value
)
SELECT
    *,
    RANK() OVER (PARTITION BY currency ORDER BY tier_value DESC NULLS FIRST) AS tier_value_rank,
    DENSE_RANK() OVER (ORDER BY tier_value DESC NULLS FIRST) AS tier_value_dense_rank,
    RANK() OVER (PARTITION BY currency ORDER BY tier_value ASC NULLS FIRST) AS tier_value_rank_2
FROM exploded

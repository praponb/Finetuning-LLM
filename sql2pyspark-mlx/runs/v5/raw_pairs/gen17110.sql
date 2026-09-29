WITH flattened AS (
    SELECT
        id,
        plan,
        effective_from,
        currency,
        EXPLODE_OUTER(tier_limits) AS tier_value
    FROM price_tiers
)
SELECT
    *,
    RANK() OVER (ORDER BY tier_value DESC NULLS LAST) AS tier_value_rank,
    STDDEV_SAMP(tier_value) OVER (PARTITION BY currency) AS currency_stddev_samp_tier_value,
    DENSE_RANK() OVER (PARTITION BY currency ORDER BY tier_value DESC NULLS LAST) AS tier_value_dense_rank
FROM flattened;

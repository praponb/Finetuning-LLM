WITH flattened AS (
    SELECT
        id,
        effective_from,
        currency,
        tier_value
    FROM price_tiers
    LATERAL VIEW EXPLODE(discounts) t AS tier_value
)
SELECT
    *,
    DENSE_RANK() OVER w_recent tier_value_dense_rank,
    PERCENT_RANK() OVER w_recent tier_value_percent_rank,
    STDDEV_SAMP(tier_value) OVER w_ordered currency_stddev_samp_tier_value
FROM flattened
WINDOW w_recent AS (PARTITION BY currency ORDER BY tier_value), w_ordered AS (PARTITION BY currency);

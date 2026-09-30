WITH lines AS (
    SELECT
        id,
        currency,
        tier_value
    FROM price_tiers
    LATERAL VIEW OUTER EXPLODE(discounts) e AS tier_value
)
SELECT
    *,
    MIN(tier_value) OVER (PARTITION BY currency) AS currency_min_tier_value,
    DENSE_RANK() OVER (PARTITION BY currency ORDER BY tier_value NULLS FIRST) AS tier_value_dense_rank
FROM lines;

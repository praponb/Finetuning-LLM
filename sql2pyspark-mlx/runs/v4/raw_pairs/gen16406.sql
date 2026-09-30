WITH lines AS (
    SELECT
        id,
        effective_from,
        tier_value
    FROM price_tiers
    LATERAL VIEW OUTER EXPLODE(tier_prices) li AS tier_value
)
SELECT
    *,
    RANK() OVER (ORDER BY tier_value) AS tier_value_rank,
    PERCENT_RANK() OVER (ORDER BY tier_value DESC NULLS LAST) AS tier_value_percent_rank,
    RANK() OVER (ORDER BY tier_value DESC NULLS LAST) AS tier_value_rank_2
FROM lines;

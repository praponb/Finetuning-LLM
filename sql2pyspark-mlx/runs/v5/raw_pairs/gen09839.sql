WITH lines AS (
    SELECT
        id,
        plan,
        effective_from,
        EXPLODE_OUTER(tier_prices) AS tier_value
    FROM price_tiers
)
SELECT
    *,
    RANK() OVER (PARTITION BY plan ORDER BY tier_value NULLS FIRST) AS tier_value_rank,
    PERCENT_RANK() OVER (ORDER BY tier_value NULLS LAST) AS tier_value_percent_rank
FROM lines
WHERE plan = 'pro';

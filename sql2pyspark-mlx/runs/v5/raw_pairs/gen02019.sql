WITH flattened_attrs AS (
    SELECT
        id,
        plan,
        effective_from,
        tier_limits,
        SLICE(tier_limits, 1, 2) AS first_two_limits,
        ELEMENT_AT(tier_prices, 1) AS entry_price,
        AGGREGATE(tier_prices, 0.0D, (acc, p) -> acc + p, acc -> ROUND(acc / SIZE(tier_prices), 2)) AS avg_tier_price
    FROM price_tiers
)
SELECT
    id,
    effective_from,
    LAST_VALUE(entry_price) IGNORE NULLS OVER (PARTITION BY plan ORDER BY effective_from ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_entry_price,
    COUNT(avg_tier_price) OVER (PARTITION BY plan) AS plan_count_avg_tier_price
FROM flattened_attrs

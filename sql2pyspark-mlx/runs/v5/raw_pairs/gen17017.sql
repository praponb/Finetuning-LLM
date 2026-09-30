WITH unpacked AS (
    -- Higher-order array functions
    SELECT
        id,
        plan,
        effective_from,
        tier_limits,
        tier_prices,
        ZIP_WITH(tier_prices, discounts, (p, d) -> ROUND(p * (1 - d), 2)) AS net_prices,
        ELEMENT_AT(tier_prices, 1) AS entry_price,
        AGGREGATE(tier_prices, CAST(0 AS DOUBLE), (acc, p) -> acc + p, acc -> ROUND(acc / SIZE(tier_prices), 2)) AS avg_tier_price,
        SLICE(tier_limits, 1, 2) AS first_two_limits
    FROM price_tiers
    WHERE effective_from <= '2025-03-28'
)
-- Running and moving aggregates
SELECT
    id,
    effective_from,
    SUM(entry_price) OVER (PARTITION BY plan ORDER BY entry_price, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_entry_price,
    RANK() OVER (PARTITION BY plan ORDER BY avg_tier_price DESC) AS avg_tier_price_rank,
    LAST_VALUE(entry_price) IGNORE NULLS OVER (ORDER BY effective_from, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_entry_price
FROM unpacked;

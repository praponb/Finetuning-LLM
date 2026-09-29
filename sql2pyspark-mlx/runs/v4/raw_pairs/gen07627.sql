WITH flattened_attrs AS (
    -- Struct fields, map lookups and JSON extraction
    SELECT
        id,
        currency,
        tier_limits,
        discounts,
        AGGREGATE(tier_prices, 0D, (acc, p) -> acc + p, acc -> ROUND(acc / SIZE(tier_prices), 2)) AS avg_tier_price,
        ARRAY_MIN(tier_prices) AS lowest_price,
        SLICE(tier_limits, 1, 2) AS first_two_limits,
        ELEMENT_AT(tier_prices, 1) AS entry_price,
        ZIP_WITH(tier_prices, discounts, (p, d) -> ROUND(p * (1 - d), 2)) AS net_prices
    FROM price_tiers
)
-- Ranking and gap features
SELECT
    id,
    currency,
    tier_limits,
    discounts,
    avg_tier_price,
    lowest_price,
    first_two_limits,
    entry_price,
    net_prices,
    LEAD(lowest_price, 1) OVER (PARTITION BY currency ORDER BY avg_tier_price, id) AS next_lowest_price,
    PERCENT_RANK() OVER (PARTITION BY currency ORDER BY entry_price DESC NULLS LAST) AS entry_price_percent_rank,
    FIRST_VALUE(entry_price, TRUE) OVER (PARTITION BY currency ORDER BY entry_price, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_entry_price
FROM flattened_attrs;

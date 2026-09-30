WITH unpacked AS (
    -- Struct fields, map lookups and JSON extraction
    SELECT
        id,
        currency,
        tier_prices,
        discounts,
        ZIP_WITH(tier_prices, discounts, (p, d) -> ROUND(p * (1 - d), 2)) AS net_prices,
        AGGREGATE(tier_prices, 0.0D, (acc, p) -> acc + p, acc -> ROUND(acc / SIZE(tier_prices), 2)) AS avg_tier_price,
        ELEMENT_AT(tier_prices, 1) AS entry_price,
        TRANSFORM(tier_prices, (p, i) -> ROUND(p * tier_limits[i], 2)) AS tier_revenue_caps
    FROM price_tiers
)
-- Window metrics over several different partitions
SELECT
    id,
    currency,
    avg_tier_price,
    COUNT(avg_tier_price) OVER w_recent AS last4_count_avg_tier_price,
    MAX(avg_tier_price) OVER w_recent AS last4_max_avg_tier_price,
    MIN(avg_tier_price) OVER w_recent AS last4_min_avg_tier_price
FROM unpacked
WHERE currency = 'THB'
WINDOW w_recent AS (PARTITION BY currency ORDER BY avg_tier_price, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW);

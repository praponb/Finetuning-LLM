WITH unpacked AS (
    -- Semi-structured parsing
    SELECT
        id,
        currency,
        SLICE(tier_limits, 1, 2) AS first_two_limits,
        ELEMENT_AT(tier_prices, 1) AS entry_price,
        AGGREGATE(tier_prices, 0D, (acc, p) -> acc + p, acc -> ROUND(acc / SIZE(tier_prices), 2)) AS avg_tier_price,
        TRANSFORM(tier_prices, (p, i) -> ROUND(p * tier_limits[i], 2)) AS tier_revenue_caps,
        ZIP_WITH(tier_prices, discounts, (p, d) -> ROUND(p * (1 - d), 2)) AS net_prices
    FROM price_tiers
)
-- Window metrics over several different partitions
SELECT
    *,
    SUM(avg_tier_price) OVER (ORDER BY entry_price, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_avg_tier_price,
    AVG(entry_price) OVER (PARTITION BY currency ORDER BY entry_price, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS entry_price_avg_6,
    LEAD(entry_price, 2) OVER (PARTITION BY currency ORDER BY entry_price, id) AS next_entry_price
FROM unpacked;

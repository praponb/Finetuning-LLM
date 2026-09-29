WITH unpacked AS (
    -- Struct fields, map lookups and JSON extraction
    SELECT
        id,
        currency,
        tier_prices,
        discounts,
        ARRAY_MIN(tier_prices) AS lowest_price,
        ZIP_WITH(tier_prices, discounts, (p, d) -> ROUND(p * (1 - d), 2)) AS net_prices,
        ELEMENT_AT(tier_prices, 1) AS entry_price,
        TRANSFORM(tier_prices, (p, i) -> ROUND(p * tier_limits[i], 2)) AS tier_revenue_caps
    FROM price_tiers
)
-- Window metrics over several different partitions
SELECT
    *,
    SUM(lowest_price) OVER (PARTITION BY currency ORDER BY lowest_price, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) running_lowest_price,
    AVG(entry_price) OVER (PARTITION BY currency ORDER BY lowest_price, id ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING) entry_price_avg_2_prior,
    MIN(lowest_price) OVER (PARTITION BY currency) currency_min_lowest_price,
    AVG(lowest_price) OVER (PARTITION BY currency ORDER BY entry_price, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) lowest_price_avg_3
FROM unpacked

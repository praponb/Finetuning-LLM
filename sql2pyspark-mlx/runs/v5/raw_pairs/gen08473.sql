WITH parsed AS (
    -- Struct fields, map lookups and JSON extraction
    SELECT
        id,
        currency,
        tier_prices,
        discounts,
        ELEMENT_AT(tier_prices, 1) AS entry_price,
        AGGREGATE(tier_prices, 0.0D, (acc, p) -> acc + p, acc -> ROUND(acc / SIZE(tier_prices), 2)) AS avg_tier_price,
        SLICE(tier_limits, 1, 2) AS first_two_limits,
        TRANSFORM(tier_prices, (p, i) -> ROUND(p * tier_limits[i], 2)) AS tier_revenue_caps
    FROM price_tiers
)
-- Grouped metrics
SELECT
    currency,
    APPROX_PERCENTILE(avg_tier_price, ARRAY(0.25, 0.5, 0.75)) AS avg_tier_price_quartiles,
    ROUND(SUM(entry_price) / NULLIF(COUNT(CASE WHEN currency = 'EUR' THEN 1 END), 0), 2) AS entry_price_per_eur
FROM parsed
GROUP BY currency
ORDER BY entry_price_per_eur DESC

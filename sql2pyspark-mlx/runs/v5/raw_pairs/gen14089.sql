WITH parsed AS (
    SELECT
        id,
        plan,
        effective_from,
        currency,
        tier_limits,
        discounts,
        SLICE(tier_limits, 1, 2) AS first_two_limits,
        ELEMENT_AT(tier_prices, 1) AS entry_price,
        ZIP_WITH(tier_prices, discounts, (p, d) -> ROUND(p * (1 - d), 2)) AS net_prices,
        AGGREGATE(tier_prices, 0.0D, (acc, p) -> acc + p, acc -> ROUND(acc / SIZE(tier_prices), 2)) AS avg_tier_price,
        TRANSFORM(tier_prices, (p, i) -> ROUND(p * tier_limits[i], 2)) AS tier_revenue_caps
    FROM price_tiers
)
SELECT
    DATE_TRUNC('QUARTER', effective_from) AS quarter_start,
    DATE_TRUNC('YEAR', effective_from) AS year_start,
    COUNT(DISTINCT currency) AS distinct_currencies,
    MIN(entry_price) AS min_entry_price,
    ROUND(SUM(avg_tier_price) / NULLIF(COUNT(CASE WHEN currency = 'USD' THEN 1 END), 0), 2) AS avg_tier_price_per_usd,
    GROUPING(DATE_TRUNC('QUARTER', effective_from)) AS grouping_level
FROM parsed
GROUP BY CUBE (DATE_TRUNC('QUARTER', effective_from), DATE_TRUNC('YEAR', effective_from))
ORDER BY min_entry_price DESC NULLS FIRST;

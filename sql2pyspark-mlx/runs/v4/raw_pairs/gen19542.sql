WITH unpacked AS (
    SELECT
        id,
        currency,
        tier_prices,
        discounts,
        ZIP_WITH(tier_prices, discounts, (p, d) -> ROUND(p * (1 - d), 2)) AS net_prices,
        TRANSFORM(tier_prices, (p, i) -> ROUND(p * tier_limits[i], 2)) AS tier_revenue_caps,
        SLICE(tier_limits, 1, 2) AS first_two_limits
    FROM price_tiers
    WHERE currency IN ('EUR', 'USD', 'THB')
)
SELECT
    currency,
    COUNT(*) FILTER (WHERE currency = 'THB') AS thb_rows,
    COUNT(*) FILTER (WHERE currency = 'USD') AS usd_rows
FROM unpacked
GROUP BY 1
HAVING COUNT(*) FILTER (WHERE currency = 'THB') < 1
ORDER BY usd_rows DESC NULLS FIRST

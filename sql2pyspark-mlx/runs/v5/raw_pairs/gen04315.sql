WITH parsed AS (
    SELECT
        id,
        plan,
        currency,
        tier_prices,
        AGGREGATE(tier_prices, 0D, (acc, p) -> acc + p, acc -> ROUND(acc / SIZE(tier_prices), 2)) AS avg_tier_price,
        TRANSFORM(tier_prices, (p, i) -> ROUND(p * tier_limits[i], 2)) AS tier_revenue_caps
    FROM price_tiers
)
SELECT
    currency,
    COUNT(DISTINCT id) AS distinct_ids,
    PERCENTILE_APPROX(avg_tier_price, 0.95) AS avg_tier_price_p95
FROM parsed
GROUP BY currency
ORDER BY avg_tier_price_p95

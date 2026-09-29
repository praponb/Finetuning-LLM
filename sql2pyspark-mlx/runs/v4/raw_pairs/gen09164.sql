WITH parsed AS (
    SELECT
        id,
        tier_limits,
        TRANSFORM(tier_prices, (p, i) -> ROUND(p * tier_limits[i], 2)) AS tier_revenue_caps,
        ARRAY_MIN(tier_prices) AS lowest_price,
        AGGREGATE(tier_prices, 0D, (acc, p) -> acc + p, acc -> ROUND(acc / SIZE(tier_prices), 2)) AS avg_tier_price
    FROM price_tiers
)
SELECT
    *,
    SUM(lowest_price) OVER (ORDER BY lowest_price, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_lowest_price,
    AVG(avg_tier_price) OVER (ORDER BY avg_tier_price, id ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING) AS avg_tier_price_avg_6_prior
FROM parsed;

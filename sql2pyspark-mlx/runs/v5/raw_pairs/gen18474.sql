WITH flattened_attrs AS (
    SELECT
        id,
        effective_from,
        currency,
        tier_prices,
        AGGREGATE(tier_prices, 0D, (acc, p) -> acc + p, acc -> ROUND(acc / SIZE(tier_prices), 2)) AS avg_tier_price,
        TRANSFORM(tier_prices, (p, i) -> ROUND(p * tier_limits[i], 2)) AS tier_revenue_caps,
        ZIP_WITH(tier_prices, discounts, (p, d) -> ROUND(p * (1 - d), 2)) AS net_prices,
        ARRAY_MIN(tier_prices) AS lowest_price
    FROM price_tiers
    WHERE currency IN ('EUR', 'THB', 'USD')
)
SELECT
    id,
    avg_tier_price,
    lowest_price,
    ROW_NUMBER() OVER (PARTITION BY currency ORDER BY lowest_price, id) AS row_seq,
    SUM(lowest_price) OVER (PARTITION BY currency ORDER BY effective_from, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_lowest_price,
    AVG(lowest_price) OVER (PARTITION BY currency ORDER BY lowest_price, id ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING) AS lowest_price_avg_6_prior,
    SUM(avg_tier_price) OVER (PARTITION BY currency ORDER BY effective_from, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_avg_tier_price
FROM flattened_attrs
WHERE currency NOT IN ('USD', 'THB');

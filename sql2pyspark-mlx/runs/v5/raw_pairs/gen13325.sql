WITH parsed AS (
    -- Semi-structured parsing
    SELECT
        id,
        currency,
        discounts,
        ZIP_WITH(tier_prices, discounts, (p, d) -> ROUND(p * (1 - d), 2)) AS net_prices,
        ARRAY_MIN(tier_prices) AS lowest_price
    FROM price_tiers
)
-- Running and moving aggregates
SELECT
    *,
    SUM(lowest_price) OVER (ORDER BY lowest_price, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_lowest_price,
    NTILE(3) OVER (ORDER BY lowest_price DESC, id) AS lowest_price_bucket,
    AVG(lowest_price) OVER (PARTITION BY currency) AS currency_avg_lowest_price,
    COUNT(lowest_price) OVER (PARTITION BY currency) AS currency_count_lowest_price
FROM parsed
WHERE lowest_price > 26;

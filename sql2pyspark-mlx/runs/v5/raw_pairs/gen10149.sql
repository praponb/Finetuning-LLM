WITH unpacked AS (
    -- Higher-order array functions
    SELECT
        id,
        effective_from,
        currency,
        tier_limits,
        discounts,
        SLICE(tier_limits, 1, 2) AS first_two_limits,
        ZIP_WITH(tier_prices, discounts, (p, d) -> ROUND(p * (1 - d), 2)) AS net_prices,
        ARRAY_MIN(tier_prices) AS lowest_price
    FROM price_tiers
    WHERE effective_from BETWEEN '2023-04-01' AND '2025-03-29'
)
-- Ranking and gap features
SELECT
    id,
    lowest_price,
    AVG(lowest_price) OVER (PARTITION BY currency ORDER BY lowest_price, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) lowest_price_avg_3,
    NTILE(4) OVER (ORDER BY lowest_price DESC, id) lowest_price_bucket
FROM unpacked;

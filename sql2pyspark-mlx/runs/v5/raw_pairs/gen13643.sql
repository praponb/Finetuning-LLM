WITH parsed AS (
    SELECT
        id,
        plan,
        currency,
        tier_limits,
        ARRAY_MIN(tier_prices) AS lowest_price,
        TRANSFORM(tier_prices, (p, i) -> ROUND(p * tier_limits[i], 2)) AS tier_revenue_caps,
        ZIP_WITH(tier_prices, discounts, (p, d) -> ROUND(p * (1 - d), 2)) AS net_prices,
        SLICE(tier_limits, 1, 2) AS first_two_limits
    FROM price_tiers
)
SELECT
    id,
    plan,
    lowest_price,
    LAG(lowest_price) OVER (PARTITION BY plan, currency ORDER BY lowest_price, id) AS prev_lowest_price,
    LEAD(lowest_price) OVER (PARTITION BY plan, currency ORDER BY lowest_price, id) AS next_lowest_price,
    ROW_NUMBER() OVER (PARTITION BY plan, currency ORDER BY lowest_price, id) AS seq_no,
    AVG(lowest_price) OVER (ORDER BY lowest_price, id ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING) AS lowest_price_avg_6_prior
FROM parsed;

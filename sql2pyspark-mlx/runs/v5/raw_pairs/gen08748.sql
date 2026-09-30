WITH unpacked AS (
    SELECT
        id,
        plan,
        tier_prices,
        discounts,
        SLICE(tier_limits, 1, 2) AS first_two_limits,
        ZIP_WITH(tier_prices, discounts, (p, d) -> ROUND(p * (1 - d), 2)) AS net_prices,
        ARRAY_MIN(tier_prices) AS lowest_price,
        TRANSFORM(tier_prices, (p, i) -> ROUND(p * tier_limits[i], 2)) AS tier_revenue_caps
    FROM price_tiers
)
SELECT
    id,
    plan,
    lowest_price,
    ROW_NUMBER() OVER (PARTITION BY plan ORDER BY lowest_price DESC, id) AS seq_no,
    LEAD(lowest_price, 2) OVER (PARTITION BY plan ORDER BY lowest_price, id) AS next_lowest_price,
    STDDEV_SAMP(lowest_price) OVER (PARTITION BY plan) AS plan_stddev_samp_lowest_price,
    LAST_VALUE(lowest_price) IGNORE NULLS OVER (PARTITION BY plan ORDER BY lowest_price, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_lowest_price
FROM unpacked

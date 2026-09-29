WITH parsed AS (
    SELECT
        id,
        plan,
        effective_from,
        currency,
        tier_limits,
        discounts,
        ARRAY_MIN(tier_prices) AS lowest_price,
        TRANSFORM(tier_prices, (p, i) -> ROUND(p * tier_limits[i], 2)) AS tier_revenue_caps
    FROM price_tiers
)
SELECT
    *,
    LEAD(lowest_price, 2, 0) OVER (ORDER BY effective_from, id) AS next_lowest_price,
    SUM(lowest_price) OVER (PARTITION BY currency ORDER BY effective_from, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_lowest_price,
    ROW_NUMBER() OVER w_part AS seq_no,
    AVG(lowest_price) OVER (PARTITION BY currency ORDER BY lowest_price, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) AS lowest_price_avg_3,
    LAG(lowest_price, 2) OVER (ORDER BY lowest_price, id) AS prev_lowest_price
FROM parsed
WINDOW w_part AS (PARTITION BY plan ORDER BY effective_from, id)

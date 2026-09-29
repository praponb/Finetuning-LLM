WITH flattened_attrs AS (
    SELECT
        id,
        plan,
        currency,
        tier_prices,
        discounts,
        AGGREGATE(tier_prices, 0D, (acc, p) -> acc + p, acc -> ROUND(acc / SIZE(tier_prices), 2)) AS avg_tier_price,
        TRANSFORM(tier_prices, (p, i) -> ROUND(p * tier_limits[i], 2)) AS tier_revenue_caps,
        ARRAY_MIN(tier_prices) AS lowest_price
    FROM price_tiers
    WHERE currency <> 'EUR'
)
SELECT
    id,
    plan,
    currency,
    ROW_NUMBER() OVER w_recent AS row_seq,
    LEAD(lowest_price, 2, 0) OVER w_ordered AS next_lowest_price,
    AVG(avg_tier_price) OVER w_ordered_2 AS avg_tier_price_avg_2,
    ROW_NUMBER() OVER w_ordered_3 AS row_seq_2,
    SUM(avg_tier_price) OVER (PARTITION BY currency ORDER BY lowest_price, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_avg_tier_price
FROM flattened_attrs
WINDOW w_recent AS (PARTITION BY currency ORDER BY avg_tier_price, id), w_ordered AS (PARTITION BY plan, currency ORDER BY avg_tier_price, id), w_ordered_2 AS (PARTITION BY currency, plan ORDER BY avg_tier_price, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW), w_ordered_3 AS (PARTITION BY plan ORDER BY lowest_price, id);

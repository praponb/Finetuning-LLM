WITH unpacked AS (
    -- Semi-structured parsing
    SELECT
        id,
        plan,
        TRANSFORM(tier_prices, (p, i) -> ROUND(p * tier_limits[i], 2)) AS tier_revenue_caps,
        ARRAY_MIN(tier_prices) AS lowest_price,
        AGGREGATE(tier_prices, CAST(0 AS DOUBLE), (acc, p) -> acc + p, acc -> ROUND(acc / SIZE(tier_prices), 2)) AS avg_tier_price,
        SLICE(tier_limits, 1, 2) AS first_two_limits
    FROM price_tiers
)
-- Ranking and gap features
SELECT
    *,
    STDDEV_SAMP(lowest_price) OVER w_ordered AS plan_stddev_samp_lowest_price,
    ROW_NUMBER() OVER w_hist AS event_seq,
    NTILE(4) OVER w_hist_2 AS lowest_price_bucket
FROM unpacked
WINDOW w_ordered AS (PARTITION BY plan), w_hist AS (PARTITION BY plan ORDER BY avg_tier_price, id), w_hist_2 AS (PARTITION BY plan ORDER BY lowest_price, id)

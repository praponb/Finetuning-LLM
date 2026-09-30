WITH flattened_attrs AS (
    -- Semi-structured parsing
    SELECT
        id,
        plan,
        tier_limits,
        tier_prices,
        TRANSFORM(tier_prices, (p, i) -> ROUND(p * tier_limits[i], 2)) AS tier_revenue_caps,
        ELEMENT_AT(tier_prices, 1) AS entry_price,
        AGGREGATE(tier_prices, 0D, (acc, p) -> acc + p, acc -> ROUND(acc / SIZE(tier_prices), 2)) AS avg_tier_price,
        SLICE(tier_limits, 1, 2) AS first_two_limits
    FROM price_tiers
)
-- Running and moving aggregates
SELECT
    *,
    ROW_NUMBER() OVER (PARTITION BY plan ORDER BY entry_price ASC, id ASC) AS seq_no,
    AVG(entry_price) OVER (PARTITION BY plan ORDER BY entry_price ASC, id ASC ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING) AS entry_price_avg_6_prior,
    AVG(entry_price) OVER (PARTITION BY plan) AS plan_avg_entry_price
FROM flattened_attrs

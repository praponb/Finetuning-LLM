WITH flattened_attrs AS (
    -- Higher-order array functions
    SELECT
        id,
        effective_from,
        currency,
        tier_limits,
        tier_prices,
        ZIP_WITH(tier_prices, discounts, (p, d) -> ROUND(p * (1 - d), 2)) AS net_prices,
        TRANSFORM(tier_prices, (p, i) -> ROUND(p * tier_limits[i], 2)) AS tier_revenue_caps,
        SLICE(tier_limits, 1, 2) AS first_two_limits
    FROM price_tiers
)
-- Window metrics over several different partitions
SELECT
    *,
    ROW_NUMBER() OVER (PARTITION BY currency ORDER BY effective_from DESC, id) seq_no,
    ROW_NUMBER() OVER (PARTITION BY currency ORDER BY effective_from DESC, id) seq_no_2
FROM flattened_attrs
WHERE effective_from >= '2024-11-01';

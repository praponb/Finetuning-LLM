WITH unpacked AS (
    -- Higher-order array functions
    SELECT
        id,
        plan,
        tier_prices,
        discounts,
        ELEMENT_AT(tier_prices, 1) AS entry_price,
        ARRAY_MIN(tier_prices) AS lowest_price,
        SLICE(tier_limits, 1, 2) AS first_two_limits,
        ZIP_WITH(tier_prices, discounts, (p, d) -> ROUND(p * (1 - d), 2)) AS net_prices,
        TRANSFORM(tier_prices, (p, i) -> ROUND(p * tier_limits[i], 2)) AS tier_revenue_caps
    FROM price_tiers
    WHERE plan = 'pro'
)
-- Window metrics over several different partitions
SELECT
    *,
    SUM(entry_price) OVER (PARTITION BY plan ORDER BY entry_price, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_entry_price,
    ROW_NUMBER() OVER (ORDER BY lowest_price DESC, id) AS event_seq,
    SUM(lowest_price) OVER (PARTITION BY plan ORDER BY lowest_price, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_lowest_price,
    FIRST_VALUE(entry_price) IGNORE NULLS OVER (PARTITION BY plan ORDER BY entry_price, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_entry_price,
    ROW_NUMBER() OVER (PARTITION BY plan ORDER BY lowest_price, id) AS event_seq_2
FROM unpacked;

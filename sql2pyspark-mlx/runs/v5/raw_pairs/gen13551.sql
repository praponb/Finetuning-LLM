WITH parsed AS (
    SELECT
        id,
        plan,
        effective_from,
        tier_prices,
        TRANSFORM(tier_prices, (p, i) -> ROUND(p * tier_limits[i], 2)) AS tier_revenue_caps,
        ZIP_WITH(tier_prices, discounts, (p, d) -> ROUND(p * (1 - d), 2)) AS net_prices,
        ELEMENT_AT(tier_prices, 1) AS entry_price,
        SLICE(tier_limits, 1, 2) AS first_two_limits
    FROM price_tiers
)
SELECT
    id,
    effective_from,
    entry_price,
    LAG(entry_price) OVER (PARTITION BY plan ORDER BY effective_from ASC, id ASC) AS prev_entry_price,
    LEAD(entry_price) OVER (PARTITION BY plan ORDER BY effective_from ASC, id ASC) AS next_entry_price,
    ROW_NUMBER() OVER (PARTITION BY plan ORDER BY effective_from ASC, id ASC) AS seq_no
FROM parsed
WHERE plan IN ('pro', 'starter', 'basic');

WITH parsed AS (
    -- Higher-order array functions
    SELECT
        id,
        effective_from,
        currency,
        tier_limits,
        ZIP_WITH(tier_prices, discounts, (p, d) -> ROUND(p * (1 - d), 2)) AS net_prices,
        SLICE(tier_limits, 1, 2) AS first_two_limits,
        ELEMENT_AT(tier_prices, 1) AS entry_price,
        ARRAY_MIN(tier_prices) AS lowest_price,
        TRANSFORM(tier_prices, (p, i) -> ROUND(p * tier_limits[i], 2)) AS tier_revenue_caps
    FROM price_tiers
    WHERE currency = 'THB'
)
-- Running and moving aggregates
SELECT
    id,
    effective_from,
    currency,
    tier_limits,
    net_prices,
    first_two_limits,
    entry_price,
    lowest_price,
    tier_revenue_caps,
    ROW_NUMBER() OVER (ORDER BY lowest_price DESC, id ASC) AS row_seq,
    ROW_NUMBER() OVER (PARTITION BY currency ORDER BY effective_from DESC, id ASC) AS seq_no,
    ROW_NUMBER() OVER (PARTITION BY currency ORDER BY entry_price ASC, id ASC) AS event_seq
FROM parsed;

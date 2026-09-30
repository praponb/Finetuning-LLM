WITH flattened_attrs AS (
    -- Semi-structured parsing
    SELECT
        id,
        plan,
        effective_from,
        tier_limits,
        tier_prices,
        discounts,
        ELEMENT_AT(tier_prices, 1) AS entry_price,
        ARRAY_MIN(tier_prices) AS lowest_price
    FROM price_tiers
)
-- Ranking and gap features
SELECT
    *,
    PERCENT_RANK() OVER w_part AS entry_price_percent_rank,
    DENSE_RANK() OVER (ORDER BY lowest_price DESC NULLS FIRST) AS lowest_price_dense_rank
FROM flattened_attrs
WINDOW w_part AS (PARTITION BY plan ORDER BY entry_price DESC);

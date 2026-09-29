WITH flattened_attrs AS (
    -- Higher-order array functions
    SELECT
        id,
        effective_from,
        currency,
        discounts,
        ZIP_WITH(tier_prices, discounts, (p, d) -> ROUND(p * (1 - d), 2)) AS net_prices,
        SLICE(tier_limits, 1, 2) AS first_two_limits
    FROM price_tiers
)
-- Window metrics over several different partitions
SELECT
    id,
    effective_from,
    currency,
    discounts,
    net_prices,
    first_two_limits,
    ROW_NUMBER() OVER (PARTITION BY currency ORDER BY effective_from, id) AS seq_no,
    ROW_NUMBER() OVER (ORDER BY effective_from DESC, id) AS event_seq
FROM flattened_attrs;

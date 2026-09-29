WITH unpacked AS (
    SELECT
        id,
        effective_from,
        tier_limits,
        discounts,
        ARRAY_MIN(tier_prices) AS lowest_price,
        SLICE(tier_limits, 1, 2) AS first_two_limits
    FROM price_tiers
)
SELECT
    id,
    lowest_price,
    LAG(lowest_price, 2) OVER (ORDER BY effective_from, id) AS prev_lowest_price,
    RANK() OVER (ORDER BY lowest_price DESC) AS lowest_price_rank
FROM unpacked

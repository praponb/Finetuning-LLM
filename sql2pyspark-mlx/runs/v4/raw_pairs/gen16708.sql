WITH unpacked AS (
    SELECT
        id,
        TRANSFORM(tier_prices, (p, i) -> ROUND(p * tier_limits[i], 2)) AS tier_revenue_caps,
        ELEMENT_AT(tier_prices, 1) AS entry_price,
        AGGREGATE(tier_prices, 0D, (acc, p) -> acc + p, acc -> ROUND(acc / SIZE(tier_prices), 2)) AS avg_tier_price,
        SLICE(tier_limits, 1, 2) AS first_two_limits,
        ARRAY_MIN(tier_prices) AS lowest_price
    FROM price_tiers
)
SELECT
    id,
    tier_revenue_caps,
    entry_price,
    avg_tier_price,
    first_two_limits,
    lowest_price,
    ROW_NUMBER() OVER (ORDER BY entry_price, id) AS row_seq,
    PERCENT_RANK() OVER (ORDER BY entry_price) AS entry_price_percent_rank,
    CUME_DIST() OVER (ORDER BY lowest_price NULLS LAST) AS lowest_price_cume_dist,
    LAST_VALUE(avg_tier_price, TRUE) OVER (ORDER BY entry_price, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_avg_tier_price
FROM unpacked

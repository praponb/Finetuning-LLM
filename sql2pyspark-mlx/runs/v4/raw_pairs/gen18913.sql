WITH flattened_attrs AS (
    SELECT
        id,
        plan,
        effective_from,
        currency,
        discounts,
        ZIP_WITH(tier_prices, discounts, (p, d) -> ROUND(p * (1 - d), 2)) AS net_prices,
        TRANSFORM(tier_prices, (p, i) -> ROUND(p * tier_limits[i], 2)) AS tier_revenue_caps,
        ARRAY_MIN(tier_prices) AS lowest_price
    FROM price_tiers
    WHERE plan <> 'starter'
)
SELECT
    *,
    FIRST_VALUE(lowest_price, TRUE) OVER (PARTITION BY plan ORDER BY lowest_price ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) first_known_lowest_price,
    CUME_DIST() OVER (ORDER BY lowest_price ASC NULLS FIRST) lowest_price_cume_dist
FROM flattened_attrs

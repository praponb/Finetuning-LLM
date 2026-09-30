SELECT
    id,
    effective_from,
    discounts,
    TRANSFORM(tier_prices, (p, i) -> ROUND(p * tier_limits[i], 2)) AS tier_revenue_caps,
    ELEMENT_AT(tier_prices, 1) AS entry_price,
    SLICE(tier_limits, 1, 2) AS first_two_limits
FROM price_tiers
WHERE effective_from BETWEEN '2024-04-01' AND '2025-09-01'

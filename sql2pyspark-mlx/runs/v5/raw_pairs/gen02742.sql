WITH flattened_attrs AS (
    SELECT
        id,
        effective_from,
        currency,
        tier_limits,
        discounts,
        ARRAY_MIN(tier_prices) AS lowest_price,
        AGGREGATE(tier_prices, 0D, (acc, p) -> acc + p, acc -> ROUND(acc / SIZE(tier_prices), 2)) AS avg_tier_price,
        ELEMENT_AT(tier_prices, 1) AS entry_price
    FROM price_tiers
)
SELECT
    currency,
    COUNT(CASE WHEN currency = 'EUR' THEN 1 END) AS eur_count,
    MAX(entry_price) FILTER (WHERE currency = 'THB') AS thb_max_entry_price,
    COUNT(*) FILTER (WHERE currency = 'USD') AS usd_rows,
    COUNT(DISTINCT currency) AS distinct_currencies,
    COUNT(*) FILTER (WHERE currency = 'USD') AS usd_rows_2
FROM flattened_attrs
GROUP BY currency
ORDER BY usd_rows NULLS LAST

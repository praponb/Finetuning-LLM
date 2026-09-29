WITH parsed AS (
    SELECT
        id,
        effective_from,
        currency,
        tier_limits,
        tier_prices,
        SLICE(tier_limits, 1, 2) AS first_two_limits,
        ZIP_WITH(tier_prices, discounts, (p, d) -> ROUND(p * (1 - d), 2)) AS net_prices
    FROM price_tiers
    WHERE MONTH(effective_from) IN (2, 8, 9)
)
SELECT
    currency,
    COUNT_IF(currency = 'EUR') AS eur_events,
    SORT_ARRAY(COLLECT_SET(currency)) AS currency_list,
    COUNT(CASE WHEN currency = 'THB' THEN 1 END) AS thb_count,
    SIZE(COLLECT_SET(currency)) AS num_currencies,
    COUNT_IF(currency = 'THB') AS thb_events
FROM parsed
GROUP BY currency
ORDER BY currency

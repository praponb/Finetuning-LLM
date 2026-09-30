WITH flattened_attrs AS (
    SELECT
        id,
        plan,
        effective_from,
        currency,
        tier_limits,
        TRANSFORM(tier_prices, (p, i) -> ROUND(p * tier_limits[i], 2)) AS tier_revenue_caps,
        ELEMENT_AT(tier_prices, 1) AS entry_price
    FROM price_tiers
    WHERE plan = 'pro'
)
SELECT
    DATE_TRUNC('MONTH', effective_from) AS activity_month,
    APPROX_PERCENTILE(entry_price, 0.5) AS entry_price_p50,
    SUM(CASE WHEN currency = 'USD' THEN entry_price ELSE 0 END) AS usd_entry_price,
    APPROX_PERCENTILE(entry_price, 0.9) AS entry_price_p90
FROM flattened_attrs
GROUP BY DATE_TRUNC('MONTH', effective_from)
ORDER BY entry_price_p50 DESC

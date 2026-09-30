WITH parsed AS (
    SELECT
        id,
        plan,
        currency,
        tier_limits,
        tier_prices,
        discounts,
        ARRAY_MIN(tier_prices) AS lowest_price,
        ELEMENT_AT(tier_prices, 1) AS entry_price
    FROM price_tiers
    WHERE plan = 'enterprise'
)
SELECT
    currency,
    plan,
    COUNT(*) FILTER (WHERE currency = 'USD') AS usd_rows,
    COUNT(DISTINCT currency) AS distinct_currencies,
    COUNT(*) FILTER (WHERE currency = 'THB') AS thb_rows,
    DENSE_RANK() OVER (PARTITION BY currency ORDER BY COUNT(*) FILTER (WHERE currency = 'THB') DESC) AS thb_rows_dense_rank
FROM parsed
GROUP BY currency, plan;

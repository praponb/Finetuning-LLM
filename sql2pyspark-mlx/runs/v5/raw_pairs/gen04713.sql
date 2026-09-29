WITH parsed AS (
    SELECT
        id,
        currency,
        tier_prices,
        ELEMENT_AT(tier_prices, 1) AS entry_price,
        ZIP_WITH(tier_prices, discounts, (p, d) -> ROUND(p * (1 - d), 2)) AS net_prices,
        TRANSFORM(tier_prices, (p, i) -> ROUND(p * tier_limits[i], 2)) AS tier_revenue_caps,
        SLICE(tier_limits, 1, 2) AS first_two_limits,
        AGGREGATE(tier_prices, 0D, (acc, p) -> acc + p, acc -> ROUND(acc / SIZE(tier_prices), 2)) AS avg_tier_price
    FROM price_tiers
)
SELECT
    currency,
    CASE WHEN avg_tier_price >= 33 THEN 'LARGE' ELSE 'SMALL' END AS avg_tier_price_tier,
    SIZE(COLLECT_SET(currency)) AS num_currencies,
    COUNT(CASE WHEN currency = 'THB' THEN 1 END) AS thb_count,
    COUNT(DISTINCT id) AS distinct_ids,
    ROUND(AVG(avg_tier_price), 2) AS avg_avg_tier_price,
    PERCENT_RANK() OVER (PARTITION BY currency ORDER BY ROUND(AVG(avg_tier_price), 2) DESC) AS avg_avg_tier_price_percent_rank
FROM parsed
GROUP BY currency, CASE WHEN avg_tier_price >= 33 THEN 'LARGE' ELSE 'SMALL' END
ORDER BY avg_tier_price_tier DESC

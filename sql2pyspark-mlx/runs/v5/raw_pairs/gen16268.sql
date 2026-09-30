WITH parsed AS (
    SELECT
        id,
        plan,
        discounts,
        SLICE(tier_limits, 1, 2) AS first_two_limits,
        ELEMENT_AT(tier_prices, 1) AS entry_price,
        ARRAY_MIN(tier_prices) AS lowest_price,
        ZIP_WITH(tier_prices, discounts, (p, d) -> ROUND(p * (1 - d), 2)) AS net_prices,
        AGGREGATE(tier_prices, CAST(0 AS DOUBLE), (acc, p) -> acc + p, acc -> ROUND(acc / SIZE(tier_prices), 2)) AS avg_tier_price
    FROM price_tiers
    WHERE plan = 'starter'
)
SELECT
    CASE WHEN entry_price >= 26 THEN 'PREMIUM' ELSE 'STANDARD' END AS entry_price_tier,
    PERCENTILE_APPROX(entry_price, ARRAY(0.25, 0.5, 0.75)) AS entry_price_quartiles,
    SUM(CASE WHEN plan = 'starter' THEN lowest_price ELSE 0 END) AS starter_lowest_price,
    MAX(lowest_price) AS max_lowest_price
FROM parsed
GROUP BY CASE WHEN entry_price >= 26 THEN 'PREMIUM' ELSE 'STANDARD' END
ORDER BY max_lowest_price DESC NULLS LAST

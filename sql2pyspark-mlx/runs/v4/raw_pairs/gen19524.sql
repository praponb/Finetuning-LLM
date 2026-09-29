WITH parsed AS (
    SELECT
        id,
        plan,
        ZIP_WITH(tier_prices, discounts, (p, d) -> ROUND(p * (1 - d), 2)) AS net_prices,
        TRANSFORM(tier_prices, (p, i) -> ROUND(p * tier_limits[i], 2)) AS tier_revenue_caps
    FROM price_tiers
    WHERE plan <> 'basic'
)
SELECT
    plan,
    COUNT(*) AS num_rows,
    SORT_ARRAY(COLLECT_SET(plan)) AS plan_list,
    SORT_ARRAY(COLLECT_SET(plan)) AS plan_list_2,
    COUNT(*) FILTER (WHERE plan = 'pro') AS pro_rows,
    GROUPING(plan) AS gid
FROM parsed
GROUP BY ROLLUP (plan)

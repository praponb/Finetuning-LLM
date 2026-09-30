WITH exploded AS (
    SELECT
        id,
        plan,
        effective_from,
        currency,
        tier_value
    FROM price_tiers
    LATERAL VIEW OUTER EXPLODE(tier_prices) li AS tier_value
)
SELECT
    currency,
    COUNT(DISTINCT id) AS distinct_ids,
    COUNT(*) AS event_count,
    COUNT(*) AS num_rows,
    SORT_ARRAY(COLLECT_SET(currency)) AS currency_list,
    SUM(CASE WHEN currency = 'USD' THEN tier_value ELSE 0 END) AS usd_tier_value,
    COUNT(*) FILTER (WHERE plan = 'enterprise') AS enterprise_rows
FROM exploded
GROUP BY currency;

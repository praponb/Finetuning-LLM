WITH lines AS (
    SELECT
        id,
        currency,
        tier_value
    FROM price_tiers
    LATERAL VIEW EXPLODE(tier_limits) x AS tier_value
),
kept_keys AS (
    SELECT currency, id
    FROM lines
    EXCEPT
    SELECT currency, id
    FROM price_tiers
    WHERE plan = 'basic'
),
kept_rows AS (
    SELECT s.*
    FROM lines s
    INNER JOIN kept_keys k ON s.currency = k.currency AND s.id = k.id
),
scored AS (
    SELECT
        *,
        PERCENT_RANK() OVER w_part tier_value_percent_rank,
        CUME_DIST() OVER (PARTITION BY currency ORDER BY tier_value NULLS LAST) tier_value_cume_dist
    FROM kept_rows
    WINDOW w_part AS (PARTITION BY currency ORDER BY tier_value DESC NULLS LAST)
),
filtered AS (
    SELECT
        id,
        currency,
        tier_value,
        tier_value_percent_rank,
        CASE WHEN tier_value >= 79 THEN 'High' WHEN tier_value >= 11 THEN 'Medium' ELSE 'Low' END tier_value_tier
    FROM scored
    WHERE currency IN ('THB', 'USD')
)
SELECT
    CASE WHEN tier_value_percent_rank >= 0.64 THEN 'LARGE' ELSE 'SMALL' END AS tier_value_percent_rank_tier,
    CASE WHEN tier_value >= 60 THEN 'PREMIUM' ELSE 'STANDARD' END AS tier_value_tier,
    COUNT(CASE WHEN currency = 'USD' THEN 1 END) AS usd_count,
    SUM(CASE WHEN currency = 'THB' THEN tier_value ELSE 0 END) AS thb_tier_value,
    COUNT(*) FILTER (WHERE currency = 'EUR') AS eur_rows,
    COUNT_IF(currency = 'EUR') AS eur_events,
    GROUPING_ID() AS grouping_level
FROM filtered
GROUP BY CUBE (CASE WHEN tier_value_percent_rank >= 0.64 THEN 'LARGE' ELSE 'SMALL' END, CASE WHEN tier_value >= 60 THEN 'PREMIUM' ELSE 'STANDARD' END)
ORDER BY tier_value_tier NULLS FIRST;

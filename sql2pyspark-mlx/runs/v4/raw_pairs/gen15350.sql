WITH exploded AS (
    SELECT
        id,
        currency,
        tier_value
    FROM price_tiers
    LATERAL VIEW EXPLODE(tier_limits) x AS tier_value
),
cleaned AS (
    SELECT
        *,
        ROUND(tier_value, 0) AS tier_value_rounded,
        COALESCE(tier_value, 0) AS tier_value_filled
    FROM exploded
    WHERE tier_value >= 41
),
scored AS (
    SELECT
        *,
        COUNT(tier_value) OVER (PARTITION BY currency) AS currency_count_tier_value,
        RANK() OVER (PARTITION BY currency ORDER BY tier_value_filled) AS tier_value_filled_rank,
        CUME_DIST() OVER (ORDER BY tier_value DESC) AS tier_value_cume_dist
    FROM cleaned
),
with_windows AS (
    SELECT
        *,
        CUME_DIST() OVER (PARTITION BY currency ORDER BY currency_count_tier_value) AS currency_count_tier_value_cume_dist,
        AVG(tier_value_cume_dist) OVER (PARTITION BY currency) AS currency_avg_tier_value_cume_dist,
        PERCENT_RANK() OVER w_grp AS tier_value_rounded_percent_rank
    FROM scored
    WINDOW w_grp AS (PARTITION BY currency ORDER BY tier_value_rounded)
)
SELECT
    CASE WHEN currency_avg_tier_value_cume_dist >= 1 THEN 'HIGH' ELSE 'LOW' END AS currency_avg_tier_value_cume_dist_tier,
    APPROX_PERCENTILE(tier_value_rounded_percent_rank, ARRAY(0.25, 0.5, 0.75)) AS tier_value_rounded_percent_rank_quartiles,
    ROUND(SUM(currency_count_tier_value) / NULLIF(COUNT(CASE WHEN currency = 'EUR' THEN 1 END), 0), 2) AS currency_count_tier_value_per_eur,
    MAX(tier_value_filled_rank) AS max_tier_value_filled_rank,
    SIZE(COLLECT_SET(currency)) AS num_currencies,
    AVG(currency_count_tier_value) AS avg_currency_count_tier_value,
    PERCENTILE_APPROX(currency_count_tier_value_cume_dist, 0.95) AS currency_count_tier_value_cume_dist_p95,
    GROUPING(CASE WHEN currency_avg_tier_value_cume_dist >= 1 THEN 'HIGH' ELSE 'LOW' END) AS grouping_id
FROM with_windows
GROUP BY CUBE (CASE WHEN currency_avg_tier_value_cume_dist >= 1 THEN 'HIGH' ELSE 'LOW' END)
HAVING MAX(tier_value_filled_rank) >= 0

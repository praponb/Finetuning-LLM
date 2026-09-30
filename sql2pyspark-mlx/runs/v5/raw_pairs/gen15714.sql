WITH flattened AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        currency,
        tier_value
    FROM price_tiers
    LATERAL VIEW EXPLODE(tier_limits) e AS tier_value
),
scored AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        DENSE_RANK() OVER (PARTITION BY currency ORDER BY tier_value NULLS LAST) AS tier_value_dense_rank,
        PERCENT_RANK() OVER (PARTITION BY currency ORDER BY tier_value DESC NULLS LAST) AS tier_value_percent_rank,
        CUME_DIST() OVER (PARTITION BY currency ORDER BY tier_value NULLS FIRST) AS tier_value_cume_dist,
        AVG(tier_value) OVER (PARTITION BY currency) AS currency_avg_tier_value
    FROM flattened
),
filtered AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        currency,
        tier_value_dense_rank,
        tier_value_percent_rank,
        tier_value_cume_dist,
        currency_avg_tier_value,
        ROUND(currency_avg_tier_value, 0) AS currency_avg_tier_value_rounded
    FROM scored
    WHERE currency_avg_tier_value BETWEEN 32 AND 50
),
scored_price_tiers AS (
    -- Running and moving aggregates
    SELECT
        *,
        COUNT(tier_value_cume_dist) OVER (PARTITION BY currency) AS currency_count_tier_value_cume_dist,
        RANK() OVER (ORDER BY tier_value_dense_rank DESC NULLS LAST) AS tier_value_dense_rank_rank
    FROM filtered
    WHERE tier_value_percent_rank >= 0.69
),
scored_price_tiers_2 AS (
    -- Ranking and gap features
    SELECT
        *,
        RANK() OVER (PARTITION BY currency ORDER BY currency_avg_tier_value DESC) AS currency_avg_tier_value_rank,
        AVG(tier_value_percent_rank) OVER w_running AS currency_avg_tier_value_percent_rank,
        DENSE_RANK() OVER (ORDER BY tier_value_cume_dist NULLS LAST) AS tier_value_cume_dist_dense_rank,
        CUME_DIST() OVER w_running_2 AS tier_value_percent_rank_cume_dist,
        MIN(currency_avg_tier_value_rounded) OVER w_running AS currency_min_currency_avg_tier_value_rounded
    FROM scored_price_tiers
    WINDOW w_running AS (PARTITION BY currency), w_running_2 AS (ORDER BY tier_value_percent_rank NULLS FIRST)
)
-- Multi-level aggregation
SELECT
    CASE WHEN currency_avg_tier_value_rank >= 7 THEN 'HIGH' ELSE 'LOW' END AS currency_avg_tier_value_rank_tier,
    CASE WHEN tier_value_cume_dist_dense_rank >= 9 THEN 'HIGH' ELSE 'LOW' END AS tier_value_cume_dist_dense_rank_tier,
    SUM(CASE WHEN currency = 'EUR' THEN tier_value_cume_dist ELSE 0 END) AS eur_tier_value_cume_dist,
    ROUND(SUM(currency_min_currency_avg_tier_value_rounded) / NULLIF(COUNT(CASE WHEN currency = 'THB' THEN 1 END), 0), 2) AS currency_min_currency_avg_tier_value_rounded_per_thb,
    COUNT(CASE WHEN currency = 'EUR' THEN 1 END) AS eur_count
FROM scored_price_tiers_2
GROUP BY GROUPING SETS ((CASE WHEN currency_avg_tier_value_rank >= 7 THEN 'HIGH' ELSE 'LOW' END, CASE WHEN tier_value_cume_dist_dense_rank >= 9 THEN 'HIGH' ELSE 'LOW' END), (CASE WHEN currency_avg_tier_value_rank >= 7 THEN 'HIGH' ELSE 'LOW' END), ())
ORDER BY eur_tier_value_cume_dist DESC

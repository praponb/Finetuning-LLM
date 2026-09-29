WITH exploded AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        effective_from,
        currency,
        EXPLODE_OUTER(discounts) AS tier_value
    FROM price_tiers
),
scored AS (
    -- Running and moving aggregates
    SELECT
        *,
        PERCENT_RANK() OVER (ORDER BY tier_value NULLS LAST) AS tier_value_percent_rank,
        RANK() OVER (ORDER BY tier_value DESC) AS tier_value_rank,
        CUME_DIST() OVER (ORDER BY tier_value NULLS LAST) AS tier_value_cume_dist,
        MIN(tier_value) OVER (PARTITION BY currency) AS currency_min_tier_value
    FROM exploded
),
metrics AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        effective_from,
        currency,
        tier_value,
        tier_value_percent_rank,
        tier_value_rank,
        tier_value_cume_dist,
        currency_min_tier_value,
        DENSE_RANK() OVER (PARTITION BY currency ORDER BY tier_value_rank NULLS LAST) AS tier_value_rank_dense_rank,
        DENSE_RANK() OVER (PARTITION BY currency ORDER BY tier_value) AS tier_value_dense_rank
    FROM scored
),
scored_price_tiers AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        ROUND(tier_value_dense_rank / SUM(tier_value_dense_rank) OVER (PARTITION BY currency), 4) AS tier_value_dense_rank_share,
        CUME_DIST() OVER (PARTITION BY currency ORDER BY tier_value_rank) AS tier_value_rank_cume_dist,
        DENSE_RANK() OVER (PARTITION BY currency ORDER BY tier_value_percent_rank NULLS LAST) AS tier_value_percent_rank_dense_rank,
        CUME_DIST() OVER (PARTITION BY currency ORDER BY tier_value NULLS FIRST) AS tier_value_cume_dist_2,
        RANK() OVER (PARTITION BY currency ORDER BY tier_value_rank DESC) AS tier_value_rank_rank
    FROM metrics
),
scored_price_tiers_2 AS (
    -- Ranking and gap features
    SELECT
        *,
        SUM(tier_value_percent_rank) OVER (PARTITION BY currency) AS currency_sum_tier_value_percent_rank,
        DENSE_RANK() OVER (PARTITION BY currency ORDER BY tier_value_cume_dist DESC) AS tier_value_cume_dist_dense_rank,
        PERCENT_RANK() OVER w_hist AS tier_value_rank_dense_rank_percent_rank
    FROM scored_price_tiers
    WINDOW w_hist AS (ORDER BY tier_value_rank_dense_rank DESC)
)
-- Multi-level aggregation
SELECT
    CASE WHEN tier_value_rank_dense_rank >= 8 THEN 'HIGH' ELSE 'LOW' END AS tier_value_rank_dense_rank_tier,
    CASE WHEN tier_value_cume_dist_dense_rank >= 9 THEN 'PREMIUM' ELSE 'STANDARD' END AS tier_value_cume_dist_dense_rank_tier,
    COUNT(*) FILTER (WHERE currency = 'USD') AS usd_rows,
    COUNT(*) FILTER (WHERE currency = 'THB') AS thb_rows,
    MAX(tier_value_percent_rank_dense_rank) FILTER (WHERE currency = 'USD') AS usd_max_tier_value_percent_rank_dense_rank,
    MAX(tier_value_rank_dense_rank_percent_rank) FILTER (WHERE currency = 'EUR') AS eur_max_tier_value_rank_dense_rank_percent_rank,
    ROUND(AVG(tier_value_dense_rank_share), 2) AS avg_tier_value_dense_rank_share,
    APPROX_PERCENTILE(currency_min_tier_value, 0.95) AS currency_min_tier_value_p95
FROM scored_price_tiers_2
GROUP BY CUBE (CASE WHEN tier_value_rank_dense_rank >= 8 THEN 'HIGH' ELSE 'LOW' END, CASE WHEN tier_value_cume_dist_dense_rank >= 9 THEN 'PREMIUM' ELSE 'STANDARD' END)

WITH flattened AS (
    SELECT
        id,
        currency,
        EXPLODE(discounts) AS val
    FROM price_tiers
),
enriched AS (
    SELECT
        *,
        RANK() OVER (PARTITION BY currency ORDER BY val) AS val_rank,
        PERCENT_RANK() OVER (PARTITION BY currency ORDER BY val NULLS FIRST) AS val_percent_rank
    FROM flattened
),
with_windows AS (
    SELECT
        id,
        currency,
        val,
        val_rank,
        val_percent_rank,
        CUME_DIST() OVER (PARTITION BY currency ORDER BY val_percent_rank NULLS FIRST) AS val_percent_rank_cume_dist,
        ROUND(val_rank / SUM(val_rank) OVER (PARTITION BY currency), 4) AS val_rank_share,
        AVG(val) OVER w_grp AS currency_avg_val,
        SUM(val) OVER w_grp AS currency_sum_val,
        CUME_DIST() OVER (PARTITION BY currency ORDER BY val_percent_rank NULLS LAST) AS val_percent_rank_cume_dist_2
    FROM enriched
    WINDOW w_grp AS (PARTITION BY currency)
),
enriched_price_tiers AS (
    SELECT
        id,
        currency,
        val,
        val_rank,
        val_percent_rank,
        val_percent_rank_cume_dist,
        val_rank_share,
        currency_avg_val,
        currency_sum_val,
        val_percent_rank_cume_dist_2,
        CUME_DIST() OVER (PARTITION BY currency ORDER BY val_percent_rank DESC NULLS LAST) AS val_percent_rank_cume_dist_3,
        MIN(currency_sum_val) OVER (PARTITION BY currency) AS currency_min_currency_sum_val
    FROM with_windows
),
with_windows_price_tiers AS (
    SELECT
        *,
        RANK() OVER (PARTITION BY currency ORDER BY val_rank_share DESC NULLS LAST) AS val_rank_share_rank,
        RANK() OVER (ORDER BY val_percent_rank NULLS FIRST) AS val_percent_rank_rank
    FROM enriched_price_tiers
),
valid AS (
    SELECT
        val_rank,
        val_percent_rank,
        val_rank_share,
        currency_avg_val,
        currency_sum_val,
        val_percent_rank_cume_dist_2,
        currency_min_currency_sum_val,
        val_rank_share_rank,
        val_percent_rank_rank,
        val_rank_share / NULLIF(currency_avg_val, 0) AS val_rank_share_per_currency_avg_val
    FROM with_windows_price_tiers
    WHERE currency = 'USD'
)
SELECT
    CASE WHEN currency_min_currency_sum_val >= 327 THEN 'PREMIUM' ELSE 'STANDARD' END AS currency_min_currency_sum_val_tier,
    CASE WHEN val_percent_rank_rank >= 9 THEN 'LARGE' ELSE 'SMALL' END AS val_percent_rank_rank_tier,
    MAX(val_percent_rank_cume_dist_2) AS max_val_percent_rank_cume_dist_2,
    PERCENTILE_APPROX(val_rank_share, 0.95) AS val_rank_share_p95,
    COUNT(*) AS row_count,
    GROUPING(CASE WHEN currency_min_currency_sum_val >= 327 THEN 'PREMIUM' ELSE 'STANDARD' END) AS grouping_level
FROM valid
GROUP BY ROLLUP (CASE WHEN currency_min_currency_sum_val >= 327 THEN 'PREMIUM' ELSE 'STANDARD' END, CASE WHEN val_percent_rank_rank >= 9 THEN 'LARGE' ELSE 'SMALL' END)
ORDER BY val_percent_rank_rank_tier

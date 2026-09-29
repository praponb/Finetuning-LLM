WITH exploded AS (
    SELECT
        id,
        currency,
        val
    FROM price_tiers
    LATERAL VIEW EXPLODE(tier_limits) x AS val
),
cleaned AS (
    SELECT
        *,
        CASE WHEN currency = 'EUR' THEN 1 ELSE 0 END AS is_eur,
        UPPER(currency) AS currency_upper
    FROM exploded
    WHERE currency = 'THB'
),
scored AS (
    SELECT
        *,
        SUM(is_eur) OVER w_running AS currency_sum_is_eur,
        CUME_DIST() OVER (PARTITION BY currency ORDER BY is_eur DESC) AS is_eur_cume_dist,
        STDDEV_SAMP(val) OVER w_running AS currency_stddev_samp_val,
        MIN(val) OVER w_running AS currency_min_val,
        PERCENT_RANK() OVER w_hist AS val_percent_rank
    FROM cleaned
    WINDOW w_running AS (PARTITION BY currency), w_hist AS (PARTITION BY currency ORDER BY val)
),
enriched AS (
    SELECT
        currency,
        currency_stddev_samp_val,
        currency_min_val,
        val_percent_rank,
        PERCENT_RANK() OVER (PARTITION BY currency ORDER BY is_eur_cume_dist DESC NULLS FIRST) AS is_eur_cume_dist_percent_rank,
        STDDEV_SAMP(is_eur) OVER w_part AS currency_stddev_samp_is_eur
    FROM scored
    WHERE currency_sum_is_eur = 181
    WINDOW w_part AS (PARTITION BY currency)
),
enriched_price_tiers AS (
    SELECT
        *,
        MAX(val_percent_rank) OVER w_recent AS currency_max_val_percent_rank,
        AVG(is_eur_cume_dist_percent_rank) OVER w_recent AS currency_avg_is_eur_cume_dist_percent_rank,
        CUME_DIST() OVER (PARTITION BY currency ORDER BY is_eur_cume_dist_percent_rank DESC) AS is_eur_cume_dist_percent_rank_cume_dist,
        CUME_DIST() OVER (PARTITION BY currency ORDER BY currency_stddev_samp_is_eur) AS currency_stddev_samp_is_eur_cume_dist,
        PERCENT_RANK() OVER (PARTITION BY currency ORDER BY val_percent_rank NULLS FIRST) AS val_percent_rank_percent_rank
    FROM enriched
    WINDOW w_recent AS (PARTITION BY currency)
),
enriched_price_tiers_2 AS (
    SELECT
        currency,
        currency_min_val,
        currency_max_val_percent_rank,
        is_eur_cume_dist_percent_rank_cume_dist,
        CUME_DIST() OVER (ORDER BY currency_stddev_samp_is_eur DESC NULLS LAST) AS currency_stddev_samp_is_eur_cume_dist_2,
        DENSE_RANK() OVER (ORDER BY currency_avg_is_eur_cume_dist_percent_rank DESC NULLS FIRST) AS currency_avg_is_eur_cume_dist_percent_rank_dense_rank
    FROM enriched_price_tiers
),
with_windows AS (
    SELECT
        *,
        AVG(currency_stddev_samp_is_eur_cume_dist_2) OVER w_grp AS currency_avg_currency_stddev_samp_is_eur_cume_dist_2,
        MAX(is_eur_cume_dist_percent_rank_cume_dist) OVER w_grp AS currency_max_is_eur_cume_dist_percent_rank_cume_dist,
        DENSE_RANK() OVER (ORDER BY is_eur_cume_dist_percent_rank_cume_dist DESC) AS is_eur_cume_dist_percent_rank_cume_dist_dense_rank,
        MIN(is_eur_cume_dist_percent_rank_cume_dist) OVER w_grp AS currency_min_is_eur_cume_dist_percent_rank_cume_dist,
        DENSE_RANK() OVER w_grp_2 AS currency_stddev_samp_is_eur_cume_dist_2_dense_rank
    FROM enriched_price_tiers_2
    WINDOW w_grp AS (PARTITION BY currency), w_grp_2 AS (ORDER BY currency_stddev_samp_is_eur_cume_dist_2 DESC)
)
SELECT
    CASE WHEN currency_min_val >= 69 THEN 'HIGH' ELSE 'LOW' END AS currency_min_val_tier,
    SORT_ARRAY(COLLECT_SET(currency)) AS currency_list,
    COUNT(CASE WHEN currency = 'EUR' THEN 1 END) AS eur_count,
    SIZE(COLLECT_SET(currency)) AS num_currencies,
    COUNT(CASE WHEN currency = 'THB' THEN 1 END) AS thb_count,
    ROUND(SUM(currency_stddev_samp_is_eur_cume_dist_2_dense_rank) / NULLIF(COUNT(CASE WHEN currency = 'THB' THEN 1 END), 0), 2) AS currency_stddev_samp_is_eur_cume_dist_2_dense_rank_per_thb
FROM with_windows
GROUP BY CASE WHEN currency_min_val >= 69 THEN 'HIGH' ELSE 'LOW' END
ORDER BY currency_min_val_tier DESC NULLS FIRST

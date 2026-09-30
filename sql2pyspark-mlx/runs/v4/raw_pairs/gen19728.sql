WITH lines AS (
    SELECT
        id,
        currency,
        EXPLODE_OUTER(discounts) AS tier_value
    FROM price_tiers
),
enriched AS (
    SELECT
        *,
        RANK() OVER (PARTITION BY currency ORDER BY tier_value DESC NULLS FIRST) AS tier_value_rank,
        MAX(tier_value) OVER (PARTITION BY currency) AS currency_max_tier_value,
        PERCENT_RANK() OVER (ORDER BY tier_value DESC) AS tier_value_percent_rank,
        DENSE_RANK() OVER (PARTITION BY currency ORDER BY tier_value DESC NULLS LAST) AS tier_value_dense_rank
    FROM lines
),
metrics AS (
    SELECT
        *,
        ROUND(tier_value_dense_rank / SUM(tier_value_dense_rank) OVER (PARTITION BY currency), 4) AS tier_value_dense_rank_share,
        PERCENT_RANK() OVER (ORDER BY tier_value NULLS FIRST) AS tier_value_percent_rank_2,
        ROUND(tier_value_rank / SUM(tier_value_rank) OVER (PARTITION BY currency), 4) AS tier_value_rank_share,
        CUME_DIST() OVER (PARTITION BY currency ORDER BY currency_max_tier_value DESC NULLS LAST) AS currency_max_tier_value_cume_dist,
        STDDEV_SAMP(tier_value_dense_rank) OVER (PARTITION BY currency) AS currency_stddev_samp_tier_value_dense_rank
    FROM enriched
),
matched_keys AS (
    SELECT currency
    FROM metrics
    EXCEPT
    SELECT currency
    FROM price_tiers
    WHERE effective_from >= '2023-03-01'
),
reconciled AS (
    SELECT s.*
    FROM metrics s
    INNER JOIN matched_keys k ON s.currency = k.currency
),
scored AS (
    SELECT
        *,
        MIN(tier_value_rank_share) OVER (PARTITION BY currency) AS currency_min_tier_value_rank_share,
        DENSE_RANK() OVER (PARTITION BY currency ORDER BY tier_value_rank_share) AS tier_value_rank_share_dense_rank
    FROM reconciled
),
filtered AS (
    SELECT *, SUBSTRING(currency, 1, 2) AS currency_prefix
    FROM scored
    WHERE currency_max_tier_value <= 69
),
enriched_price_tiers AS (
    SELECT
        *,
        CUME_DIST() OVER w_grp AS tier_value_percent_rank_2_cume_dist,
        ROUND(currency_stddev_samp_tier_value_dense_rank / SUM(currency_stddev_samp_tier_value_dense_rank) OVER (PARTITION BY currency), 4) AS currency_stddev_samp_tier_value_dense_rank_share
    FROM filtered
    WINDOW w_grp AS (ORDER BY tier_value_percent_rank_2 DESC NULLS FIRST)
)
SELECT
    CASE WHEN currency_max_tier_value_cume_dist >= 0.74 THEN 'PREMIUM' ELSE 'STANDARD' END AS currency_max_tier_value_cume_dist_tier,
    CASE WHEN tier_value_dense_rank_share >= 0.69 THEN 'PREMIUM' ELSE 'STANDARD' END AS tier_value_dense_rank_share_tier,
    currency,
    SUM(CASE WHEN currency = 'EUR' THEN tier_value_dense_rank ELSE 0 END) AS eur_tier_value_dense_rank,
    SUM(tier_value_rank_share_dense_rank) FILTER (WHERE currency = 'USD') AS usd_total_tier_value_rank_share_dense_rank,
    GROUPING(CASE WHEN currency_max_tier_value_cume_dist >= 0.74 THEN 'PREMIUM' ELSE 'STANDARD' END) AS gid
FROM enriched_price_tiers
GROUP BY GROUPING SETS ((CASE WHEN currency_max_tier_value_cume_dist >= 0.74 THEN 'PREMIUM' ELSE 'STANDARD' END, CASE WHEN tier_value_dense_rank_share >= 0.69 THEN 'PREMIUM' ELSE 'STANDARD' END, currency), (CASE WHEN currency_max_tier_value_cume_dist >= 0.74 THEN 'PREMIUM' ELSE 'STANDARD' END, CASE WHEN tier_value_dense_rank_share >= 0.69 THEN 'PREMIUM' ELSE 'STANDARD' END), (CASE WHEN currency_max_tier_value_cume_dist >= 0.74 THEN 'PREMIUM' ELSE 'STANDARD' END));

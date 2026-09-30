WITH lines AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        effective_from,
        currency,
        EXPLODE(discounts) AS val
    FROM price_tiers
),
valid AS (
    -- Keep recent, valid rows
    SELECT
        *,
        DAYOFWEEK(effective_from) effective_from_dayofweek,
        ROUND(val * 0.9, 2) val_scaled
    FROM lines
    WHERE effective_from >= TO_DATE('2024-05-14')
),
scored AS (
    -- Running and moving aggregates
    SELECT
        id,
        currency,
        val_scaled,
        STDDEV_SAMP(val) OVER (PARTITION BY currency) currency_stddev_samp_val,
        AVG(val) OVER (PARTITION BY currency) currency_avg_val,
        COUNT(val) OVER (PARTITION BY currency) currency_count_val,
        RANK() OVER (ORDER BY val NULLS FIRST) val_rank
    FROM valid
),
cleaned AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        currency,
        val_scaled,
        currency_avg_val,
        currency_count_val,
        val_rank,
        CAST(val_scaled AS INT) val_scaled_int,
        SUBSTRING(currency, 1, 3) currency_prefix
    FROM scored
    WHERE val_scaled <= 42
),
enriched AS (
    -- Running and moving aggregates
    SELECT
        *,
        ROUND(currency_count_val / SUM(currency_count_val) OVER (PARTITION BY currency), 4) currency_count_val_share,
        CUME_DIST() OVER (PARTITION BY currency ORDER BY val_rank) val_rank_cume_dist
    FROM cleaned
),
filtered AS (
    -- Basic cleansing and defaults
    SELECT
        *,
        CAST(currency_count_val AS DOUBLE) currency_count_val_str,
        TRIM(currency) clean_currency
    FROM enriched
    WHERE val_rank BETWEEN 3 AND 7
),
scored_price_tiers AS (
    -- Ranking and gap features
    SELECT
        *,
        PERCENT_RANK() OVER (ORDER BY val_rank_cume_dist DESC NULLS LAST) val_rank_cume_dist_percent_rank,
        PERCENT_RANK() OVER (PARTITION BY currency ORDER BY val_rank_cume_dist DESC NULLS LAST) val_rank_cume_dist_percent_rank_2,
        STDDEV_SAMP(val_scaled) OVER (PARTITION BY currency) currency_stddev_samp_val_scaled
    FROM filtered
)
-- Multi-level aggregation
SELECT
    CASE WHEN val_rank_cume_dist_percent_rank_2 >= 0.85 THEN 'HIGH' ELSE 'LOW' END AS val_rank_cume_dist_percent_rank_2_tier,
    AVG(val_rank_cume_dist_percent_rank) FILTER (WHERE currency = 'EUR') AS eur_avg_val_rank_cume_dist_percent_rank,
    PERCENTILE_APPROX(currency_stddev_samp_val_scaled, 0.95) AS currency_stddev_samp_val_scaled_p95,
    AVG(val_rank_cume_dist_percent_rank) AS avg_val_rank_cume_dist_percent_rank
FROM scored_price_tiers
GROUP BY CASE WHEN val_rank_cume_dist_percent_rank_2 >= 0.85 THEN 'HIGH' ELSE 'LOW' END
HAVING eur_avg_val_rank_cume_dist_percent_rank < 1000000
ORDER BY currency_stddev_samp_val_scaled_p95

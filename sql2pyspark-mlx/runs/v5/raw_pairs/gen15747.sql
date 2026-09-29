WITH exploded AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        effective_from,
        currency,
        val
    FROM price_tiers
    LATERAL VIEW OUTER EXPLODE(tier_limits) li AS val
),
enriched AS (
    -- Ranking and gap features
    SELECT
        id,
        effective_from,
        MAX(val) OVER w_hist AS currency_max_val,
        MIN(val) OVER w_hist AS currency_min_val,
        DENSE_RANK() OVER (PARTITION BY currency ORDER BY val DESC NULLS FIRST) AS val_dense_rank,
        DENSE_RANK() OVER w_recent AS val_dense_rank_2,
        CUME_DIST() OVER w_recent_2 AS val_cume_dist
    FROM exploded
    WINDOW w_hist AS (PARTITION BY currency), w_recent AS (PARTITION BY currency ORDER BY val DESC NULLS LAST), w_recent_2 AS (PARTITION BY currency ORDER BY val NULLS FIRST)
),
metrics AS (
    -- Running and moving aggregates
    SELECT
        currency_max_val,
        currency_min_val,
        val_dense_rank,
        PERCENT_RANK() OVER (ORDER BY currency_max_val DESC NULLS LAST) AS currency_max_val_percent_rank,
        RANK() OVER (ORDER BY val_dense_rank) AS val_dense_rank_rank,
        RANK() OVER w_ordered AS currency_max_val_rank
    FROM enriched
    WHERE val_dense_rank = 8
    WINDOW w_ordered AS (ORDER BY currency_max_val DESC)
),
base AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        currency_max_val,
        currency_min_val,
        val_dense_rank,
        currency_max_val_percent_rank,
        val_dense_rank_rank,
        currency_max_val_rank,
        COALESCE(currency_min_val, 0) AS currency_min_val_filled
    FROM metrics
    WHERE currency_max_val_percent_rank BETWEEN 0.27 AND 0.49
)
-- Multi-level aggregation
SELECT
    CASE WHEN currency_max_val_rank >= 7 THEN 'LARGE' ELSE 'SMALL' END AS currency_max_val_rank_tier,
    PERCENTILE_APPROX(val_dense_rank_rank, 0.9) AS val_dense_rank_rank_p90,
    ROUND(AVG(currency_min_val_filled), 2) AS avg_currency_min_val_filled
FROM base
GROUP BY CASE WHEN currency_max_val_rank >= 7 THEN 'LARGE' ELSE 'SMALL' END
ORDER BY val_dense_rank_rank_p90

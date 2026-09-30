WITH exploded AS (
    SELECT
        id,
        currency,
        val
    FROM price_tiers
    LATERAL VIEW OUTER EXPLODE(tier_limits) t AS val
),
with_windows AS (
    SELECT
        *,
        DENSE_RANK() OVER (PARTITION BY currency ORDER BY val) AS val_dense_rank,
        AVG(val) OVER (PARTITION BY currency) AS currency_avg_val,
        CUME_DIST() OVER (PARTITION BY currency ORDER BY val DESC NULLS FIRST) AS val_cume_dist
    FROM exploded
),
valid AS (
    SELECT *, currency_avg_val / NULLIF(val_cume_dist, 0) AS currency_avg_val_per_val_cume_dist
    FROM with_windows
    WHERE val_dense_rank <> 6
)
SELECT
    id,
    currency,
    val,
    val_dense_rank,
    currency_avg_val,
    val_cume_dist,
    currency_avg_val_per_val_cume_dist,
    DENSE_RANK() OVER (ORDER BY currency_avg_val DESC NULLS FIRST) AS currency_avg_val_dense_rank,
    PERCENT_RANK() OVER (PARTITION BY currency ORDER BY val_cume_dist DESC) AS val_cume_dist_percent_rank
FROM valid;

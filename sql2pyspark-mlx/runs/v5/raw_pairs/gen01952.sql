WITH lines AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        currency,
        EXPLODE_OUTER(tier_limits) AS val
    FROM price_tiers
),
scored AS (
    -- Ranking and gap features
    SELECT
        id,
        currency,
        RANK() OVER w_hist AS val_rank,
        DENSE_RANK() OVER w_part AS val_dense_rank,
        CUME_DIST() OVER (ORDER BY val ASC NULLS FIRST) AS val_cume_dist,
        RANK() OVER w_hist AS val_rank_2
    FROM lines
    WHERE currency = 'USD'
    WINDOW w_hist AS (PARTITION BY currency ORDER BY val ASC NULLS FIRST), w_part AS (ORDER BY val DESC NULLS LAST)
),
metrics AS (
    -- Running and moving aggregates
    SELECT
        *,
        AVG(val_cume_dist) OVER (PARTITION BY currency) AS currency_avg_val_cume_dist,
        ROUND(val_dense_rank / SUM(val_dense_rank) OVER (PARTITION BY currency), 4) AS val_dense_rank_share,
        ROUND(val_rank / SUM(val_rank) OVER (PARTITION BY currency), 4) AS val_rank_share,
        DENSE_RANK() OVER (ORDER BY val_dense_rank DESC NULLS LAST) AS val_dense_rank_dense_rank,
        ROUND(val_rank_2 / SUM(val_rank_2) OVER (PARTITION BY currency), 4) AS val_rank_2_share
    FROM scored
),
valid AS (
    -- Keep recent, valid rows
    SELECT
        currency,
        val_rank,
        val_dense_rank,
        val_cume_dist,
        currency_avg_val_cume_dist,
        val_dense_rank_share,
        val_rank_2_share,
        CASE WHEN currency = 'USD' THEN 1 ELSE 0 END AS is_usd
    FROM metrics
    WHERE currency <> 'THB'
),
metrics_price_tiers AS (
    -- Ranking and gap features
    SELECT
        *,
        CUME_DIST() OVER w_part AS val_cume_dist_cume_dist,
        CUME_DIST() OVER w_recent AS val_rank_cume_dist,
        MAX(currency_avg_val_cume_dist) OVER w_part_2 AS currency_max_currency_avg_val_cume_dist
    FROM valid
    WINDOW w_part AS (PARTITION BY currency ORDER BY val_cume_dist DESC NULLS FIRST), w_recent AS (PARTITION BY currency ORDER BY val_rank DESC NULLS LAST), w_part_2 AS (PARTITION BY currency)
),
with_windows AS (
    -- Ranking and gap features
    SELECT
        currency,
        val_dense_rank,
        val_cume_dist,
        currency_avg_val_cume_dist,
        is_usd,
        val_cume_dist_cume_dist,
        RANK() OVER (PARTITION BY currency ORDER BY currency_max_currency_avg_val_cume_dist ASC NULLS LAST) AS currency_max_currency_avg_val_cume_dist_rank,
        DENSE_RANK() OVER (PARTITION BY currency ORDER BY val_dense_rank_share DESC NULLS LAST) AS val_dense_rank_share_dense_rank,
        DENSE_RANK() OVER (ORDER BY currency_max_currency_avg_val_cume_dist DESC) AS currency_max_currency_avg_val_cume_dist_dense_rank,
        RANK() OVER (ORDER BY val_cume_dist ASC) AS val_cume_dist_rank,
        SUM(currency_avg_val_cume_dist) OVER (PARTITION BY currency) AS currency_sum_currency_avg_val_cume_dist
    FROM metrics_price_tiers
)
SELECT
    currency,
    val_dense_rank,
    currency_avg_val_cume_dist,
    is_usd,
    val_cume_dist_cume_dist,
    val_dense_rank_share_dense_rank,
    val_cume_dist_rank
FROM with_windows
ORDER BY currency DESC NULLS FIRST;

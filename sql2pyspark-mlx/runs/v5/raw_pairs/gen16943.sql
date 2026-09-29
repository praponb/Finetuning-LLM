WITH deduped AS (
    -- Keep one row per key (latest first)
    SELECT *
    FROM promotions
    QUALIFY ROW_NUMBER() OVER (PARTITION BY channel, promo_code ORDER BY discount_pct, id) = 1
),
scored AS (
    -- Ranking and gap features
    SELECT
        *,
        CUME_DIST() OVER (PARTITION BY promo_code ORDER BY discount_pct DESC) AS discount_pct_cume_dist,
        LAST_VALUE(channel) IGNORE NULLS OVER (PARTITION BY channel, promo_code ORDER BY starts_on, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_channel
    FROM deduped
),
metrics AS (
    -- Running and moving aggregates
    SELECT
        id,
        promo_code,
        starts_on,
        MAX(discount_pct_cume_dist) OVER w_running AS running_max_discount_pct_cume_dist,
        MIN(discount_pct_cume_dist) OVER w_running AS running_min_discount_pct_cume_dist,
        SUM(discount_pct_cume_dist) OVER w_running AS running_sum_discount_pct_cume_dist,
        AVG(discount_pct_cume_dist) OVER w_running AS running_avg_discount_pct_cume_dist,
        STDDEV_SAMP(discount_pct) OVER (PARTITION BY channel) AS channel_stddev_samp_discount_pct
    FROM scored
    WHERE channel IN ('web', 'partner', 'store')
    WINDOW w_running AS (PARTITION BY channel ORDER BY discount_pct, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
),
latest AS (
    -- QUALIFY-style window filter
    SELECT *
    FROM metrics
    QUALIFY ROW_NUMBER() OVER (PARTITION BY promo_code ORDER BY running_max_discount_pct_cume_dist DESC NULLS LAST, id) = 1
),
with_windows AS (
    -- Running and moving aggregates
    SELECT
        id,
        starts_on,
        running_min_discount_pct_cume_dist,
        running_sum_discount_pct_cume_dist,
        FIRST_VALUE(running_max_discount_pct_cume_dist) IGNORE NULLS OVER (PARTITION BY promo_code ORDER BY channel_stddev_samp_discount_pct, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_running_max_discount_pct_cume_dist,
        NTILE(10) OVER (PARTITION BY promo_code ORDER BY running_max_discount_pct_cume_dist, id) AS running_max_discount_pct_cume_dist_bucket,
        NTILE(3) OVER (PARTITION BY promo_code ORDER BY channel_stddev_samp_discount_pct, id) AS channel_stddev_samp_discount_pct_bucket,
        ROW_NUMBER() OVER (PARTITION BY promo_code ORDER BY running_max_discount_pct_cume_dist DESC, id) AS seq_no
    FROM latest
),
with_windows_promotions AS (
    -- Running and moving aggregates
    SELECT
        *,
        LAG(channel_stddev_samp_discount_pct_bucket) OVER w_recent AS prev_channel_stddev_samp_discount_pct_bucket,
        LEAD(channel_stddev_samp_discount_pct_bucket) OVER w_recent AS next_channel_stddev_samp_discount_pct_bucket,
        LAG(seq_no) OVER w_recent AS prev_seq_no,
        ROW_NUMBER() OVER w_recent AS seq_no_2,
        ROW_NUMBER() OVER (ORDER BY seq_no, id) AS row_seq
    FROM with_windows
    WINDOW w_recent AS (ORDER BY running_max_discount_pct_cume_dist_bucket, id)
),
totals AS (
    -- Grouped metrics
    SELECT
        CASE WHEN channel_stddev_samp_discount_pct_bucket >= 7 THEN 'HIGH' ELSE 'LOW' END AS channel_stddev_samp_discount_pct_bucket_tier,
        COUNT(DISTINCT id) AS distinct_ids,
        MAX(next_channel_stddev_samp_discount_pct_bucket) AS max_next_channel_stddev_samp_discount_pct_bucket,
        AVG(seq_no_2) AS avg_seq_no_2,
        MAX(prev_channel_stddev_samp_discount_pct_bucket) AS max_prev_channel_stddev_samp_discount_pct_bucket
    FROM with_windows_promotions
    GROUP BY 1
)
SELECT channel_stddev_samp_discount_pct_bucket_tier
FROM totals

WITH enriched AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        starts_on,
        ends_on,
        COUNT(discount_pct) OVER (PARTITION BY channel) AS channel_count_discount_pct,
        DENSE_RANK() OVER (ORDER BY discount_pct) AS discount_pct_dense_rank,
        LAG(discount_pct, 1, 0) OVER (ORDER BY starts_on, id) AS prev_discount_pct,
        SUM(discount_pct) OVER (PARTITION BY channel ORDER BY ends_on, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_discount_pct,
        DENSE_RANK() OVER (PARTITION BY channel ORDER BY discount_pct DESC) AS discount_pct_dense_rank_2
    FROM promotions
    WHERE discount_pct > 0.27
),
metrics AS (
    -- Running and moving aggregates
    SELECT
        *,
        ROW_NUMBER() OVER w_ordered AS event_seq,
        AVG(channel_count_discount_pct) OVER w_hist AS channel_count_discount_pct_avg_2,
        AVG(discount_pct_dense_rank) OVER w_hist AS discount_pct_dense_rank_avg_2
    FROM enriched
    WINDOW w_ordered AS (ORDER BY discount_pct_dense_rank_2 DESC, id), w_hist AS (ORDER BY prev_discount_pct, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW)
),
metrics_promotions AS (
    -- Running and moving aggregates
    SELECT
        *,
        SUM(discount_pct_dense_rank) OVER (ORDER BY prev_discount_pct, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_discount_pct_dense_rank,
        SUM(channel_count_discount_pct) OVER (ORDER BY prev_discount_pct, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_channel_count_discount_pct,
        AVG(channel_count_discount_pct_avg_2) OVER (ORDER BY cumulative_discount_pct, id ROWS BETWEEN 5 PRECEDING AND CURRENT ROW) AS channel_count_discount_pct_avg_2_avg_5,
        LEAD(channel_count_discount_pct, 2) OVER (ORDER BY channel_count_discount_pct, id) AS next_channel_count_discount_pct
    FROM metrics
),
metrics_promotions_2 AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        ends_on,
        discount_pct_dense_rank,
        cumulative_discount_pct,
        discount_pct_dense_rank_2,
        channel_count_discount_pct_avg_2,
        channel_count_discount_pct_avg_2_avg_5,
        next_channel_count_discount_pct,
        ROW_NUMBER() OVER (ORDER BY discount_pct_dense_rank_avg_2, id) AS event_seq_2,
        CUME_DIST() OVER (ORDER BY running_discount_pct_dense_rank DESC NULLS FIRST) AS running_discount_pct_dense_rank_cume_dist,
        DENSE_RANK() OVER (ORDER BY discount_pct_dense_rank) AS discount_pct_dense_rank_dense_rank,
        NTILE(3) OVER (ORDER BY prev_discount_pct DESC, id) AS prev_discount_pct_bucket
    FROM metrics_promotions
)
SELECT
    id,
    ends_on,
    discount_pct_dense_rank,
    cumulative_discount_pct,
    discount_pct_dense_rank_2,
    channel_count_discount_pct_avg_2,
    channel_count_discount_pct_avg_2_avg_5,
    next_channel_count_discount_pct,
    event_seq_2
FROM metrics_promotions_2
ORDER BY next_channel_count_discount_pct DESC NULLS FIRST, ends_on;

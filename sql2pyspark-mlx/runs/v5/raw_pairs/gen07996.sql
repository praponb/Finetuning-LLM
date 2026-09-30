WITH base AS (
    SELECT
        id,
        supplier_name,
        city,
        ROUND(rating * 1.07, 2) rating_adjusted
    FROM suppliers
    WHERE city IS NOT NULL
      AND city IS NOT DISTINCT FROM 'Singapore'
),
enriched AS (
    SELECT
        id,
        supplier_name,
        rating_adjusted,
        AVG(rating_adjusted) OVER (PARTITION BY city ORDER BY rating_adjusted, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) rating_adjusted_avg_2,
        RANK() OVER (PARTITION BY city ORDER BY rating_adjusted DESC NULLS LAST) rating_adjusted_rank,
        ROW_NUMBER() OVER (PARTITION BY city ORDER BY rating_adjusted, id) event_seq
    FROM base
),
scored AS (
    SELECT
        *,
        AVG(event_seq) OVER (ORDER BY rating_adjusted_rank, id ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING) event_seq_avg_2_prior,
        LAG(rating_adjusted_avg_2, 2) OVER w_running prev_rating_adjusted_avg_2,
        NTILE(10) OVER (ORDER BY rating_adjusted_rank, id) rating_adjusted_rank_bucket
    FROM enriched
    WINDOW w_running AS (ORDER BY rating_adjusted_avg_2, id)
),
with_windows AS (
    SELECT
        *,
        LEAD(prev_rating_adjusted_avg_2) OVER (ORDER BY event_seq_avg_2_prior, id) next_prev_rating_adjusted_avg_2,
        LEAD(rating_adjusted_rank) OVER (ORDER BY event_seq_avg_2_prior, id) next_rating_adjusted_rank,
        ROW_NUMBER() OVER (ORDER BY event_seq_avg_2_prior, id) row_seq,
        LAG(rating_adjusted_rank, 1) OVER (ORDER BY event_seq_avg_2_prior, id) prev_rating_adjusted_rank
    FROM scored
),
summary AS (
    SELECT
        CASE WHEN prev_rating_adjusted_rank >= 7 THEN 'PREMIUM' ELSE 'STANDARD' END AS prev_rating_adjusted_rank_tier,
        CASE WHEN event_seq_avg_2_prior >= 6 THEN 'LARGE' ELSE 'SMALL' END AS event_seq_avg_2_prior_tier,
        CASE WHEN next_prev_rating_adjusted_avg_2 >= 74 THEN 'LARGE' ELSE 'SMALL' END AS next_prev_rating_adjusted_avg_2_tier,
        APPROX_PERCENTILE(rating_adjusted_rank, ARRAY(0.25, 0.5, 0.75)) AS rating_adjusted_rank_quartiles,
        COUNT(DISTINCT id) AS distinct_ids,
        MAX(row_seq) AS max_row_seq,
        COUNT(*) AS event_count,
        SUM(event_seq_avg_2_prior) AS total_event_seq_avg_2_prior,
        PERCENTILE_APPROX(row_seq, ARRAY(0.25, 0.5, 0.75)) AS row_seq_quartiles,
        PERCENT_RANK() OVER (PARTITION BY CASE WHEN prev_rating_adjusted_rank >= 7 THEN 'PREMIUM' ELSE 'STANDARD' END ORDER BY MAX(row_seq) DESC) AS max_row_seq_percent_rank
    FROM with_windows
    GROUP BY prev_rating_adjusted_rank_tier, event_seq_avg_2_prior_tier, next_prev_rating_adjusted_avg_2_tier
)
SELECT
    prev_rating_adjusted_rank_tier,
    event_seq_avg_2_prior_tier,
    next_prev_rating_adjusted_avg_2_tier,
    rating_adjusted_rank_quartiles,
    distinct_ids,
    max_row_seq,
    row_seq_quartiles,
    max_row_seq_percent_rank
FROM summary;

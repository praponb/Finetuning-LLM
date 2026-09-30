WITH cleaned AS (
    SELECT
        id,
        channel,
        promo_code,
        discount_pct,
        TRIM(channel) AS clean_channel
    FROM promotions
    WHERE YEAR(ends_on) = 2023
),
with_windows AS (
    SELECT
        *,
        LEAD(discount_pct) OVER w_recent AS next_discount_pct,
        LAG(discount_pct) OVER w_recent AS prev_discount_pct,
        ROW_NUMBER() OVER w_recent AS event_seq,
        COUNT(discount_pct) OVER (PARTITION BY channel) AS channel_count_discount_pct
    FROM cleaned
    WHERE discount_pct > 0.3
    WINDOW w_recent AS (PARTITION BY promo_code ORDER BY discount_pct, id)
),
with_windows_promotions AS (
    SELECT
        id,
        channel,
        discount_pct,
        event_seq,
        COUNT(next_discount_pct) OVER (PARTITION BY channel ORDER BY discount_pct, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_count_next_discount_pct,
        SUM(next_discount_pct) OVER (PARTITION BY channel ORDER BY discount_pct, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_sum_next_discount_pct,
        AVG(next_discount_pct) OVER (PARTITION BY channel ORDER BY discount_pct, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_avg_next_discount_pct,
        AVG(event_seq) OVER (PARTITION BY promo_code) AS promo_code_avg_event_seq
    FROM with_windows
),
enriched AS (
    SELECT
        id,
        running_count_next_discount_pct,
        running_sum_next_discount_pct,
        promo_code_avg_event_seq,
        SUM(running_count_next_discount_pct) OVER (PARTITION BY channel ORDER BY discount_pct, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_running_count_next_discount_pct,
        RANK() OVER (ORDER BY running_avg_next_discount_pct) AS running_avg_next_discount_pct_rank
    FROM with_windows_promotions
),
valid AS (
    SELECT
        *,
        CAST(running_count_next_discount_pct AS DOUBLE) AS running_count_next_discount_pct_num,
        running_sum_next_discount_pct * promo_code_avg_event_seq AS running_sum_next_discount_pct_times_promo_code_avg_event_seq
    FROM enriched
    WHERE running_sum_next_discount_pct BETWEEN 1.46 AND 1.54
),
scored AS (
    SELECT
        *,
        SUM(running_sum_next_discount_pct) OVER (ORDER BY running_count_next_discount_pct_num, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_running_sum_next_discount_pct,
        LEAD(running_avg_next_discount_pct_rank, 1) OVER (ORDER BY cumulative_running_count_next_discount_pct, id) AS next_running_avg_next_discount_pct_rank,
        ROW_NUMBER() OVER (ORDER BY cumulative_running_count_next_discount_pct, id) AS seq_no,
        AVG(promo_code_avg_event_seq) OVER (ORDER BY running_avg_next_discount_pct_rank, id ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING) AS promo_code_avg_event_seq_avg_6_prior
    FROM valid
)
SELECT
    seq_no,
    CASE WHEN cumulative_running_count_next_discount_pct >= 85 THEN 'HIGH' ELSE 'LOW' END AS cumulative_running_count_next_discount_pct_tier,
    CASE WHEN running_avg_next_discount_pct_rank >= 8 THEN 'HIGH' ELSE 'LOW' END AS running_avg_next_discount_pct_rank_tier,
    COUNT(DISTINCT id) AS distinct_ids,
    PERCENTILE_APPROX(running_avg_next_discount_pct_rank, ARRAY(0.25, 0.5, 0.75)) AS running_avg_next_discount_pct_rank_quartiles,
    COUNT(*) AS num_rows,
    APPROX_PERCENTILE(cumulative_running_sum_next_discount_pct, 0.95) AS cumulative_running_sum_next_discount_pct_p95,
    ROUND(AVG(running_sum_next_discount_pct_times_promo_code_avg_event_seq), 2) AS avg_running_sum_next_discount_pct_times_promo_code_avg_event_seq,
    MIN(running_avg_next_discount_pct_rank) AS min_running_avg_next_discount_pct_rank,
    DENSE_RANK() OVER (PARTITION BY seq_no ORDER BY COUNT(*) DESC) AS num_rows_dense_rank
FROM scored
GROUP BY seq_no, CASE WHEN cumulative_running_count_next_discount_pct >= 85 THEN 'HIGH' ELSE 'LOW' END, CASE WHEN running_avg_next_discount_pct_rank >= 8 THEN 'HIGH' ELSE 'LOW' END;

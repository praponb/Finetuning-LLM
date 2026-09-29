WITH cleaned AS (
    SELECT *, ROUND(duration_sec, 0) duration_sec_rounded
    FROM web_events
    WHERE duration_sec >= 349
),
metrics AS (
    SELECT
        *,
        ROW_NUMBER() OVER (PARTITION BY page ORDER BY duration_sec DESC, id) row_seq,
        SUM(duration_sec_rounded) OVER (PARTITION BY event_type ORDER BY CAST(event_time AS BIGINT) RANGE BETWEEN 604800 PRECEDING AND CURRENT ROW) duration_sec_rounded_sum_7d,
        COUNT(duration_sec) OVER (ORDER BY CAST(event_time AS BIGINT) RANGE BETWEEN 7776000 PRECEDING AND CURRENT ROW) duration_sec_count_90d,
        AVG(duration_sec_rounded) OVER (PARTITION BY event_type ORDER BY duration_sec, id ROWS BETWEEN 2 PRECEDING AND 1 PRECEDING) duration_sec_rounded_avg_2_prior,
        MAX(duration_sec_rounded) OVER (ORDER BY CAST(event_time AS BIGINT) RANGE BETWEEN 604800 PRECEDING AND CURRENT ROW) duration_sec_rounded_max_7d
    FROM cleaned
),
metrics_web_events AS (
    SELECT
        id,
        session_id,
        event_type,
        page,
        event_time,
        duration_sec,
        duration_sec_rounded,
        row_seq,
        duration_sec_rounded_sum_7d,
        duration_sec_count_90d,
        duration_sec_rounded_avg_2_prior,
        duration_sec_rounded_max_7d,
        RANK() OVER (PARTITION BY event_type ORDER BY duration_sec_count_90d) duration_sec_count_90d_rank,
        FIRST_VALUE(duration_sec_rounded_sum_7d, TRUE) OVER (PARTITION BY session_id ORDER BY duration_sec_rounded_max_7d, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) first_known_duration_sec_rounded_sum_7d
    FROM metrics
),
with_prev AS (
    SELECT *, LAG(event_time) OVER (PARTITION BY session_id ORDER BY event_time, id) AS prev_event_time
    FROM metrics_web_events
),
flagged AS (
    SELECT *, CASE WHEN prev_event_time IS NULL OR event_time > prev_event_time + INTERVAL 30 DAYS THEN 1 ELSE 0 END AS session_start_flag
    FROM with_prev
),
sessionized AS (
    SELECT *, SUM(session_start_flag) OVER (PARTITION BY session_id ORDER BY event_time, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_no
    FROM flagged
),
enriched AS (
    SELECT
        id,
        session_id,
        event_type,
        page,
        event_time,
        duration_sec,
        duration_sec_rounded,
        row_seq,
        duration_sec_rounded_sum_7d,
        duration_sec_count_90d,
        duration_sec_rounded_avg_2_prior,
        duration_sec_rounded_max_7d,
        duration_sec_count_90d_rank,
        first_known_duration_sec_rounded_sum_7d,
        prev_event_time,
        session_start_flag,
        session_no,
        COUNT(duration_sec_rounded_sum_7d) OVER (PARTITION BY session_id) session_id_count_duration_sec_rounded_sum_7d,
        NTILE(10) OVER (PARTITION BY page ORDER BY duration_sec_count_90d_rank DESC, id) duration_sec_count_90d_rank_bucket
    FROM sessionized
)
SELECT
    *,
    SUM(row_seq) OVER (PARTITION BY session_id ORDER BY duration_sec_rounded_max_7d, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) running_row_seq,
    MAX(duration_sec) OVER (PARTITION BY page, event_type) page_max_duration_sec
FROM enriched

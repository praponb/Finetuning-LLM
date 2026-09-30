WITH cleaned AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        *,
        TO_DATE(event_time) AS event_time_date,
        COALESCE(page, 'OTHER') AS resolved_page
    FROM web_events
    WHERE page IS NOT NULL
      AND duration_sec = 162
),
metrics AS (
    -- Running and moving aggregates
    SELECT
        id,
        session_id,
        page,
        event_time,
        duration_sec,
        LAG(duration_sec) OVER w_running AS prev_duration_sec,
        ROW_NUMBER() OVER w_running AS row_seq,
        SUM(duration_sec) OVER w_hist AS duration_sec_sum_7d
    FROM cleaned
    WHERE duration_sec != 346
    WINDOW w_running AS (PARTITION BY session_id ORDER BY duration_sec, id), w_hist AS (PARTITION BY session_id ORDER BY CAST(event_time AS BIGINT) RANGE BETWEEN 604800 PRECEDING AND CURRENT ROW)
),
with_windows AS (
    -- Running and moving aggregates
    SELECT
        *,
        RANK() OVER (ORDER BY duration_sec DESC NULLS FIRST) AS duration_sec_rank,
        ROW_NUMBER() OVER (PARTITION BY page ORDER BY prev_duration_sec, id) AS row_seq_2
    FROM metrics
),
with_windows_web_events AS (
    -- Ranking and gap features
    SELECT
        *,
        SUM(row_seq) OVER w_part AS last4_sum_row_seq,
        COUNT(row_seq) OVER w_part AS last4_count_row_seq,
        MIN(row_seq) OVER w_part AS last4_min_row_seq
    FROM with_windows
    WINDOW w_part AS (PARTITION BY session_id ORDER BY duration_sec_rank, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW)
)
-- Aggregate with conditional metrics
SELECT
    session_id,
    DATE_TRUNC('YEAR', event_time) AS year_start,
    APPROX_PERCENTILE(last4_sum_row_seq, ARRAY(0.25, 0.5, 0.75)) AS last4_sum_row_seq_quartiles,
    PERCENTILE_APPROX(duration_sec_rank, 0.9) AS duration_sec_rank_p90,
    COUNT(DISTINCT session_id) AS distinct_session_ids,
    SUM(duration_sec_rank) FILTER (WHERE page = '/search') AS search_total_duration_sec_rank,
    SIZE(COLLECT_SET(page)) AS num_pages,
    SUM(row_seq) FILTER (WHERE page = '/search') AS search_total_row_seq
FROM with_windows_web_events
GROUP BY CUBE (session_id, DATE_TRUNC('YEAR', event_time))
ORDER BY year_start DESC;

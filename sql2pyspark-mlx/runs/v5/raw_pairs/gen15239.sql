WITH with_windows AS (
    SELECT
        id,
        page,
        event_time,
        AVG(duration_sec) OVER (PARTITION BY session_id ORDER BY CAST(event_time AS BIGINT) RANGE BETWEEN 2592000 PRECEDING AND CURRENT ROW) AS duration_sec_avg_30d,
        LEAD(duration_sec, 1) OVER (PARTITION BY session_id, page ORDER BY duration_sec, id) AS next_duration_sec,
        LEAD(duration_sec, 1, 0) OVER (PARTITION BY page, event_type ORDER BY event_time, id) AS next_duration_sec_2,
        SUM(duration_sec) OVER (PARTITION BY event_type ORDER BY duration_sec, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_duration_sec,
        ROW_NUMBER() OVER (PARTITION BY session_id ORDER BY duration_sec, id) AS seq_no
    FROM web_events
),
flagged AS (
    SELECT *, CASE WHEN LAG(event_time) OVER (PARTITION BY page ORDER BY event_time, id) IS NULL
                 OR event_time > LAG(event_time) OVER (PARTITION BY page ORDER BY event_time, id) + INTERVAL '60' DAY THEN 1 ELSE 0 END AS is_new_burst
    FROM with_windows
),
islands AS (
    SELECT *, SUM(is_new_burst) OVER (PARTITION BY page ORDER BY event_time, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS island_id
    FROM flagged
),
flagged_2 AS (
    SELECT *, CASE WHEN event_time - LAG(event_time) OVER (PARTITION BY page ORDER BY event_time, id) > INTERVAL 7 DAYS THEN 1 ELSE 0 END AS is_new_burst_2
    FROM islands
),
sessions_web_events AS (
    SELECT *, SUM(is_new_burst_2) OVER (PARTITION BY page ORDER BY event_time, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS island_id_2
    FROM flagged_2
),
metrics AS (
    SELECT
        id,
        page,
        next_duration_sec,
        running_duration_sec,
        island_id_2,
        AVG(next_duration_sec_2) OVER w_grp AS last4_avg_next_duration_sec_2,
        SUM(next_duration_sec_2) OVER w_grp AS last4_sum_next_duration_sec_2,
        COUNT(next_duration_sec_2) OVER w_grp AS last4_count_next_duration_sec_2,
        ROW_NUMBER() OVER (PARTITION BY page ORDER BY island_id DESC, id) AS row_seq
    FROM sessions_web_events
    WINDOW w_grp AS (PARTITION BY page ORDER BY next_duration_sec, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW)
),
scored AS (
    SELECT
        *,
        LAG(next_duration_sec, 2) OVER (PARTITION BY page ORDER BY island_id_2, id) AS prev_next_duration_sec,
        NTILE(5) OVER (PARTITION BY page ORDER BY last4_avg_next_duration_sec_2 DESC, id) AS last4_avg_next_duration_sec_2_bucket
    FROM metrics
),
valid AS (
    SELECT
        id,
        page,
        next_duration_sec,
        island_id_2,
        last4_sum_next_duration_sec_2,
        last4_count_next_duration_sec_2,
        row_seq,
        prev_next_duration_sec,
        last4_avg_next_duration_sec_2_bucket,
        COALESCE(running_duration_sec, 0) AS running_duration_sec_filled
    FROM scored
    WHERE last4_avg_next_duration_sec_2 < 200
),
base AS (
    SELECT
        id,
        page,
        island_id_2,
        last4_sum_next_duration_sec_2,
        last4_count_next_duration_sec_2,
        row_seq,
        running_duration_sec_filled,
        CEIL(prev_next_duration_sec) AS prev_next_duration_sec_whole,
        ROUND(last4_sum_next_duration_sec_2 * 0.85, 2) AS last4_sum_next_duration_sec_2_scaled,
        COALESCE(page, 'NONE') AS resolved_page
    FROM valid
    WHERE page IS NOT NULL
)
SELECT
    page,
    SUM(CASE WHEN page = '/home' THEN prev_next_duration_sec_whole ELSE 0 END) AS home_prev_next_duration_sec_whole,
    SORT_ARRAY(COLLECT_SET(page)) AS page_list,
    ROUND(AVG(row_seq), 2) AS avg_row_seq
FROM base
GROUP BY page
HAVING SUM(CASE WHEN page = '/home' THEN prev_next_duration_sec_whole ELSE 0 END) > 0
ORDER BY avg_row_seq DESC NULLS LAST;

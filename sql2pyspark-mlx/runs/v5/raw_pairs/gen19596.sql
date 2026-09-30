WITH metrics AS (
    SELECT
        *,
        LEAD(duration_sec, 1, 0) OVER (PARTITION BY session_id ORDER BY duration_sec, id) AS next_duration_sec,
        CUME_DIST() OVER w_grp AS duration_sec_cume_dist
    FROM web_events
    WINDOW w_grp AS (PARTITION BY event_type ORDER BY duration_sec NULLS FIRST)
),
enriched AS (
    SELECT
        a.id,
        a.session_id,
        a.page,
        a.event_time,
        a.duration_sec,
        p.browser,
        p.device
    FROM metrics a
    JOIN sessions p
        ON a.session_id = p.id
),
metrics_web_events AS (
    SELECT
        *,
        LAG(duration_sec) OVER w_ordered AS prev_duration_sec,
        LEAD(duration_sec) OVER w_ordered AS next_duration_sec,
        ROW_NUMBER() OVER w_ordered AS event_seq,
        NTILE(5) OVER (PARTITION BY browser ORDER BY duration_sec, id) AS duration_sec_bucket
    FROM enriched
    WINDOW w_ordered AS (PARTITION BY device, session_id ORDER BY event_time, id)
),
metrics_web_events_2 AS (
    SELECT
        *,
        SUM(next_duration_sec) OVER (PARTITION BY device) AS device_sum_next_duration_sec,
        NTILE(10) OVER (PARTITION BY session_id, device ORDER BY next_duration_sec DESC, id) AS next_duration_sec_bucket,
        LAG(event_seq, 1, 0) OVER (PARTITION BY page ORDER BY event_seq, id) AS prev_event_seq
    FROM metrics_web_events
),
with_windows AS (
    SELECT
        *,
        SUM(duration_sec) OVER (PARTITION BY session_id ORDER BY event_time, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_duration_sec,
        AVG(next_duration_sec) OVER w_hist AS next_duration_sec_avg_5_prior,
        DENSE_RANK() OVER (PARTITION BY page ORDER BY duration_sec DESC NULLS FIRST) AS duration_sec_dense_rank
    FROM metrics_web_events_2
    WHERE prev_event_seq <> 3
    WINDOW w_hist AS (PARTITION BY browser ORDER BY prev_duration_sec, id ROWS BETWEEN 5 PRECEDING AND 1 PRECEDING)
)
SELECT
    CASE WHEN next_duration_sec_bucket >= 7 THEN 'LARGE' ELSE 'SMALL' END AS next_duration_sec_bucket_tier,
    DATE_TRUNC('WEEK', event_time) AS week_start,
    SORT_ARRAY(COLLECT_SET(device)) AS device_list,
    (UNIX_TIMESTAMP(MAX(event_time)) - UNIX_TIMESTAMP(MIN(event_time))) / 60 AS duration_min,
    SIZE(COLLECT_SET(browser)) AS num_browsers,
    MAX(event_seq) FILTER (WHERE page = '/home') AS home_max_event_seq,
    MIN(next_duration_sec_avg_5_prior) AS min_next_duration_sec_avg_5_prior
FROM with_windows
GROUP BY 1, 2
ORDER BY duration_min NULLS FIRST

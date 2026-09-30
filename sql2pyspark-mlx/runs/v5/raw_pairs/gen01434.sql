WITH cleaned AS (
    SELECT
        id,
        session_id,
        event_type,
        page,
        event_time,
        duration_sec,
        ROUND(duration_sec, 2) AS duration_sec_rounded
    FROM web_events
    WHERE event_time > TIMESTAMP '2023-10-17 10:00:00'
),
latest AS (
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY session_id ORDER BY duration_sec_rounded DESC, id) AS rank_in_group
        FROM cleaned
    ) t
    WHERE rank_in_group = 1
),
metrics AS (
    SELECT
        *,
        RANK() OVER (PARTITION BY page ORDER BY duration_sec_rounded) AS duration_sec_rounded_rank,
        COUNT(duration_sec) OVER w_recent AS page_count_duration_sec
    FROM latest
    WINDOW w_recent AS (PARTITION BY page)
),
with_gaps AS (
    SELECT *, CASE WHEN event_time - LAG(event_time) OVER (PARTITION BY session_id ORDER BY event_time, id) > INTERVAL 14 DAYS THEN 1 ELSE 0 END AS new_group_flag
    FROM metrics
),
sessionized AS (
    SELECT
        id,
        session_id,
        page,
        event_time,
        duration_sec_rounded,
        rank_in_group,
        SUM(new_group_flag) OVER (PARTITION BY session_id ORDER BY event_time, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_no
    FROM with_gaps
),
cleaned_web_events AS (
    SELECT
        id,
        page,
        event_time,
        duration_sec_rounded,
        rank_in_group,
        session_no,
        UPPER(page) AS page_upper,
        DATE_FORMAT(event_time, 'yyyy-MM') AS event_time_label
    FROM sessionized
    WHERE page IS DISTINCT FROM '/cart'
      AND event_time >= DATE '2023-09-27'
),
metrics_web_events AS (
    SELECT
        id,
        page,
        session_no,
        LEAD(duration_sec_rounded) OVER (PARTITION BY page ORDER BY event_time, id) AS next_duration_sec_rounded,
        LEAD(session_no) OVER (PARTITION BY page ORDER BY event_time, id) AS next_session_no,
        LAG(session_no) OVER (PARTITION BY page ORDER BY event_time, id) AS prev_session_no,
        ROW_NUMBER() OVER (PARTITION BY page ORDER BY event_time, id) AS event_seq,
        SUM(duration_sec_rounded) OVER (PARTITION BY page ORDER BY rank_in_group, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_duration_sec_rounded
    FROM cleaned_web_events
),
deduped AS (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY page ORDER BY session_no DESC, id) AS rank_in_group
    FROM metrics_web_events
    QUALIFY rank_in_group = 1
)
SELECT
    CASE WHEN next_duration_sec_rounded >= 80 THEN 'PREMIUM' ELSE 'STANDARD' END AS next_duration_sec_rounded_tier,
    session_no,
    APPROX_PERCENTILE(session_no, 0.95) AS session_no_p95,
    ROUND(AVG(next_session_no), 2) AS avg_next_session_no,
    ROUND(AVG(next_session_no), 2) AS avg_next_session_no_2
FROM deduped
GROUP BY CASE WHEN next_duration_sec_rounded >= 80 THEN 'PREMIUM' ELSE 'STANDARD' END, session_no
ORDER BY session_no DESC;

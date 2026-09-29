WITH enriched AS (
    SELECT
        e.*,
        d.browser,
        d.country
    FROM web_events e
    LEFT JOIN sessions d
        ON e.session_id = d.id
),
latest AS (
    SELECT *, RANK() OVER (PARTITION BY country ORDER BY duration_sec NULLS LAST) AS latest_rank
    FROM enriched
    QUALIFY latest_rank = 1
),
with_gaps AS (
    SELECT *, CASE WHEN LAG(event_time) OVER (PARTITION BY country ORDER BY event_time, id) IS NULL
                 OR event_time > LAG(event_time) OVER (PARTITION BY country ORDER BY event_time, id) + INTERVAL 7 DAYS THEN 1 ELSE 0 END AS is_new_session
    FROM latest
),
sessions_web_events AS (
    SELECT
        id,
        event_type,
        event_time,
        duration_sec,
        browser,
        country,
        latest_rank,
        SUM(is_new_session) OVER (PARTITION BY country ORDER BY event_time, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS burst_id
    FROM with_gaps
),
scored AS (
    SELECT
        *,
        LAG(duration_sec, 1) OVER w_hist AS prev_duration_sec,
        AVG(duration_sec) OVER (PARTITION BY event_type ORDER BY duration_sec, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS duration_sec_avg_2
    FROM sessions_web_events
    WHERE latest_rank >= 1
    WINDOW w_hist AS (PARTITION BY browser ORDER BY latest_rank, id)
),
with_gaps_2 AS (
    SELECT *, CASE WHEN event_time - LAG(event_time) OVER (PARTITION BY country ORDER BY event_time, id) > INTERVAL 90 DAYS THEN 1 ELSE 0 END AS session_start_flag
    FROM scored
),
sessions_web_events_2 AS (
    SELECT *, SUM(session_start_flag) OVER (PARTITION BY country ORDER BY event_time, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_id
    FROM with_gaps_2
)
SELECT
    DATE_TRUNC('YEAR', event_time) AS year_start,
    SUM(CASE WHEN event_type = 'click' THEN burst_id ELSE 0 END) AS click_burst_id,
    ROUND(AVG(duration_sec_avg_2), 2) AS avg_duration_sec_avg_2,
    (UNIX_TIMESTAMP(MAX(event_time)) - UNIX_TIMESTAMP(MIN(event_time))) / 60 AS span_minutes,
    ROUND(AVG(burst_id), 2) AS avg_burst_id,
    SUM(session_id) AS total_session_id
FROM sessions_web_events_2
GROUP BY DATE_TRUNC('YEAR', event_time)
HAVING avg_duration_sec_avg_2 > 0
ORDER BY year_start NULLS LAST;

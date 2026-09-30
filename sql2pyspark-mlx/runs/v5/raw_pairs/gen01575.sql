WITH base AS (
    SELECT
        *,
        SUBSTRING(page, 1, 4) AS page_prefix,
        COALESCE(page, 'N/A') AS page_filled
    FROM web_events
    WHERE duration_sec IS NULL
),
with_gaps AS (
    SELECT *, CASE WHEN LAG(event_time) OVER (PARTITION BY session_id ORDER BY event_time, id) IS NULL
                 OR event_time > LAG(event_time) OVER (PARTITION BY session_id ORDER BY event_time, id) + INTERVAL '30' DAY THEN 1 ELSE 0 END AS is_new_session
    FROM base
),
islands AS (
    SELECT *, SUM(is_new_session) OVER (PARTITION BY session_id ORDER BY event_time, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS burst_id
    FROM with_gaps
),
deduped AS (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY event_type ORDER BY is_new_session DESC NULLS FIRST, id) AS rank_in_group
    FROM islands
    QUALIFY rank_in_group = 1
),
cleaned AS (
    SELECT
        id,
        page,
        event_time,
        duration_sec,
        page_prefix,
        page_filled,
        is_new_session,
        burst_id,
        rank_in_group,
        CAST(event_time AS DATE) AS event_time_day
    FROM deduped
    WHERE event_time > TO_DATE('2023-06-04')
      AND rank_in_group > 1
),
with_gaps_2 AS (
    SELECT *, CASE WHEN event_time - LAG(event_time) OVER (PARTITION BY page ORDER BY event_time, id) > INTERVAL '14' DAY THEN 1 ELSE 0 END AS is_new_burst
    FROM cleaned
),
sessions_web_events AS (
    SELECT *, SUM(is_new_burst) OVER (PARTITION BY page ORDER BY event_time, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS burst_id_2
    FROM with_gaps_2
),
metrics AS (
    SELECT
        *,
        LEAD(burst_id_2) OVER w_part AS next_burst_id_2,
        LAG(burst_id_2) OVER w_part AS prev_burst_id_2,
        LAG(is_new_session) OVER w_part AS prev_is_new_session,
        LEAD(is_new_session) OVER w_part AS next_is_new_session,
        ROW_NUMBER() OVER w_part AS row_seq,
        SUM(burst_id) OVER (ORDER BY is_new_session, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_burst_id
    FROM sessions_web_events
    WINDOW w_part AS (PARTITION BY page ORDER BY event_time, id)
),
flagged AS (
    SELECT *, CASE WHEN LAG(event_time) OVER (PARTITION BY page ORDER BY event_time, id) IS NULL
                 OR event_time > LAG(event_time) OVER (PARTITION BY page ORDER BY event_time, id) + INTERVAL 7 DAYS THEN 1 ELSE 0 END AS is_new_session_2
    FROM metrics
),
islands_web_events AS (
    SELECT *, SUM(is_new_session_2) OVER (PARTITION BY page ORDER BY event_time, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS burst_id_3
    FROM flagged
)
SELECT
    cumulative_burst_id,
    CASE WHEN is_new_burst >= 1 THEN 'HIGH' ELSE 'LOW' END AS is_new_burst_tier,
    DATE_TRUNC('MONTH', event_time) AS activity_month,
    COUNT(*) FILTER (WHERE page = '/home') AS home_rows,
    SIZE(COLLECT_SET(page)) AS num_pages,
    (UNIX_TIMESTAMP(MAX(event_time)) - UNIX_TIMESTAMP(MIN(event_time))) / 60 AS duration_min,
    COUNT(*) AS event_count,
    COUNT(*) AS row_count
FROM islands_web_events
GROUP BY CUBE (cumulative_burst_id, CASE WHEN is_new_burst >= 1 THEN 'HIGH' ELSE 'LOW' END, DATE_TRUNC('MONTH', event_time))
HAVING (UNIX_TIMESTAMP(MAX(event_time)) - UNIX_TIMESTAMP(MIN(event_time))) / 60 > 0
ORDER BY num_pages NULLS FIRST;

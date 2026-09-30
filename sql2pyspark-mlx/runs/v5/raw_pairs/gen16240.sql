WITH filtered AS (
    SELECT
        *,
        CASE WHEN event_type IN ('click', 'page_view') THEN 'Core' ELSE 'Other' END AS event_type_group,
        COALESCE(duration_sec, 0) AS duration_sec_filled
    FROM web_events
    WHERE event_time > DATE '2023-06-14'
      AND event_time >= '2023-05-01'
),
enriched AS (
    SELECT
        s.*,
        p.country,
        p.browser
    FROM filtered s
    LEFT JOIN sessions p
        ON s.session_id = p.id
),
joined AS (
    SELECT
        a.*,
        s.country AS session_country,
        s.device
    FROM enriched a
    JOIN sessions s
        ON a.session_id = s.id
),
with_prev AS (
    SELECT *, LAG(event_time) OVER (PARTITION BY event_type ORDER BY event_time, id) AS prev_event_time
    FROM joined
),
gaps AS (
    SELECT *, CASE WHEN prev_event_time IS NULL OR event_time > prev_event_time + INTERVAL '30' DAY THEN 1 ELSE 0 END AS session_start_flag
    FROM with_prev
),
sessions_web_events AS (
    SELECT *, SUM(session_start_flag) OVER (PARTITION BY event_type ORDER BY event_time, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_id_2
    FROM gaps
)
SELECT
    id,
    session_id,
    event_time,
    duration_sec,
    event_type_group,
    country,
    session_country,
    device,
    prev_event_time
FROM sessions_web_events
ORDER BY session_id DESC;

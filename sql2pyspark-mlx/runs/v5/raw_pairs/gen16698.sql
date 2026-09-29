WITH cleaned AS (
    SELECT
        id,
        device,
        started_at,
        country,
        HOUR(started_at) AS started_at_hour
    FROM sessions
    WHERE customer_id IS NOT NULL
),
scored AS (
    SELECT
        id,
        device,
        started_at,
        country,
        started_at_hour,
        AVG(started_at_hour) OVER (PARTITION BY country ORDER BY started_at_hour, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS last7_avg_started_at_hour,
        MAX(started_at_hour) OVER (PARTITION BY country ORDER BY started_at_hour, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS last7_max_started_at_hour,
        MIN(started_at_hour) OVER (PARTITION BY country ORDER BY started_at_hour, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS last7_min_started_at_hour,
        SUM(started_at_hour) OVER (PARTITION BY country ORDER BY started_at_hour, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS last7_sum_started_at_hour
    FROM cleaned
),
latest_match AS (
    SELECT
        s.*,
        latest.duration_sec,
        latest.web_event_event_type
    FROM scored s
    JOIN LATERAL (
            SELECT c.duration_sec, c.event_type AS web_event_event_type
            FROM web_events c
            WHERE c.session_id = s.id
              AND c.event_time <= s.started_at
            ORDER BY c.event_time DESC, c.id DESC
            LIMIT 1
        ) latest ON TRUE
),
latest AS (
    SELECT
        id,
        device,
        started_at_hour,
        last7_max_started_at_hour,
        last7_min_started_at_hour,
        last7_sum_started_at_hour,
        duration_sec,
        web_event_event_type
    FROM latest_match
    QUALIFY ROW_NUMBER() OVER (PARTITION BY device ORDER BY started_at_hour DESC, id) = 1
),
not_flagged AS (
    SELECT s.*
    FROM latest s
    WHERE NOT EXISTS (
            SELECT 1
            FROM web_events x
            WHERE x.session_id = s.id
              AND x.event_time < '2025-05-01'
        )
)
SELECT
    id,
    device,
    started_at_hour,
    last7_max_started_at_hour,
    last7_min_started_at_hour,
    last7_sum_started_at_hour,
    duration_sec,
    web_event_event_type
FROM not_flagged
ORDER BY last7_min_started_at_hour DESC, device DESC

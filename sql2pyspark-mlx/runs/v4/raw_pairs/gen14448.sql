WITH with_dims AS (
    SELECT
        s.*,
        b.country,
        b.device,
        b.browser
    FROM web_events s
    INNER JOIN sessions b
        ON s.session_id = b.id
),
cleaned AS (
    SELECT
        id,
        session_id,
        event_type,
        page,
        event_time,
        country,
        device,
        browser,
        DATE_TRUNC('YEAR', event_time) AS event_time_period
    FROM with_dims
    WHERE device IS NOT NULL
),
deduped AS (
    SELECT *, RANK() OVER (PARTITION BY country ORDER BY event_time NULLS FIRST) AS rn
    FROM cleaned
    QUALIFY rn = 1
),
prev_events AS (
    SELECT *, LAG(event_time) OVER (PARTITION BY page ORDER BY event_time, id) AS prev_event_time
    FROM deduped
),
flagged AS (
    SELECT *, CASE WHEN prev_event_time IS NULL OR event_time > prev_event_time + INTERVAL '90' DAY THEN 1 ELSE 0 END AS session_start_flag
    FROM prev_events
),
sessions_web_events AS (
    SELECT *, SUM(session_start_flag) OVER (PARTITION BY page ORDER BY event_time, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS burst_id
    FROM flagged
),
enriched AS (
    SELECT e.*, b.device AS session_device
    FROM sessions_web_events e
    INNER JOIN sessions b
        ON e.session_id = b.id
),
deduped_web_events AS (
    SELECT *
    FROM enriched
    QUALIFY ROW_NUMBER() OVER (PARTITION BY browser ORDER BY rn, id) = 1
)
SELECT *
FROM (
    SELECT
        session_id,
        device,
        event_type,
        session_start_flag
    FROM deduped_web_events
) src
PIVOT (
    SUM(session_start_flag) AS value_sum,
    COUNT(1) AS item_count
    FOR event_type IN ('add_to_cart', 'page_view')
)

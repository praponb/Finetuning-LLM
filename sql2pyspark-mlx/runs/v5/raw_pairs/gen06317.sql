WITH enriched AS (
    -- Attach sessions attributes
    SELECT
        src.id,
        src.session_id,
        src.page,
        src.event_time,
        p.country,
        p.device
    FROM web_events src
    INNER JOIN sessions p
        ON src.session_id = p.id
),
deduped AS (
    -- QUALIFY-style window filter
    SELECT
        id,
        page,
        event_time,
        country,
        device
    FROM enriched
    QUALIFY ROW_NUMBER() OVER (PARTITION BY country ORDER BY event_time DESC, id) = 1
),
ranked AS (
    -- Deduplicate with ROW_NUMBER
    SELECT id, event_time
    FROM (
        SELECT *, RANK() OVER (PARTITION BY device, page ORDER BY event_time DESC NULLS LAST) AS rank_in_group
        FROM deduped
    ) x
    WHERE rank_in_group = 1
),
cleaned AS (
    -- Basic cleansing and defaults
    SELECT
        *,
        DATEDIFF(CURRENT_DATE(), event_time) AS days_since_event_time,
        CAST(event_time AS DATE) AS event_time_day
    FROM ranked
    WHERE event_time > TIMESTAMP '2023-05-07 09:00:00'
),
filtered AS (
    -- Keep recent, valid rows
    SELECT *, HOUR(event_time) AS event_time_hour
    FROM cleaned
    WHERE event_time >= TIMESTAMP '2023-04-22 08:00:00'
),
filtered_web_events AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        event_time,
        event_time_day,
        event_time_hour,
        days_since_event_time - event_time_hour AS days_since_event_time_minus_event_time_hour
    FROM filtered
    WHERE event_time >= TIMESTAMP '2023-04-28 01:00:00'
),
enriched_web_events AS (
    -- Running and moving aggregates
    SELECT
        id,
        event_time_hour,
        LAST_VALUE(event_time_hour) IGNORE NULLS OVER (ORDER BY days_since_event_time_minus_event_time_hour, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_event_time_hour,
        DENSE_RANK() OVER (ORDER BY event_time_hour DESC NULLS LAST) AS event_time_hour_dense_rank,
        LAST_VALUE(event_time_hour, TRUE) OVER (ORDER BY event_time_day, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_event_time_hour_2,
        SUM(event_time_hour) OVER (ORDER BY event_time, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_event_time_hour
    FROM filtered_web_events
)
-- Aggregate with conditional metrics
SELECT
    CASE WHEN running_event_time_hour >= 360 THEN 'LARGE' ELSE 'SMALL' END AS running_event_time_hour_tier,
    CASE WHEN last_known_event_time_hour_2 >= 71 THEN 'LARGE' ELSE 'SMALL' END AS last_known_event_time_hour_2_tier,
    COUNT(*) AS cnt,
    ROUND(AVG(last_known_event_time_hour_2), 2) AS avg_last_known_event_time_hour_2,
    SUM(event_time_hour_dense_rank) AS total_event_time_hour_dense_rank,
    MAX(last_known_event_time_hour) AS max_last_known_event_time_hour
FROM enriched_web_events
GROUP BY CASE WHEN running_event_time_hour >= 360 THEN 'LARGE' ELSE 'SMALL' END, CASE WHEN last_known_event_time_hour_2 >= 71 THEN 'LARGE' ELSE 'SMALL' END
HAVING avg_last_known_event_time_hour_2 < 1000000

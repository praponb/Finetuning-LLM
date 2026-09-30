WITH cleaned AS (
    SELECT *, CAST(event_time AS DATE) AS event_time_day
    FROM web_events
    WHERE event_time >= DATE '2023-08-06'
),
cleaned_web_events AS (
    SELECT
        id,
        session_id,
        event_type,
        page,
        event_time,
        event_time_day,
        ROUND(duration_sec, 1) AS duration_sec_rounded
    FROM cleaned
    WHERE page IS NOT NULL
),
deduped AS (
    SELECT
        id,
        session_id,
        event_type,
        event_time_day,
        duration_sec_rounded
    FROM (
        SELECT *, RANK() OVER (PARTITION BY event_type, session_id ORDER BY event_time NULLS FIRST) AS rank_in_group
        FROM cleaned_web_events
    ) x
    WHERE rank_in_group = 1
),
ranked AS (
    SELECT *
    FROM deduped
    QUALIFY ROW_NUMBER() OVER (PARTITION BY session_id ORDER BY duration_sec_rounded, id) = 1
),
kept_keys AS (
    SELECT session_id, id
    FROM ranked
    EXCEPT
    SELECT session_id, id
    FROM web_events
    WHERE session_id IN (8, 15, 17)
),
verified AS (
    SELECT s.*
    FROM ranked s
    JOIN kept_keys k ON s.session_id = k.session_id AND s.id = k.id
)
SELECT
    session_id,
    CASE WHEN duration_sec_rounded >= 73 THEN 'LARGE' ELSE 'SMALL' END AS duration_sec_rounded_tier,
    DATE_TRUNC('QUARTER', event_time_day) AS quarter_start,
    SUM(duration_sec_rounded) AS total_duration_sec_rounded,
    COUNT(*) AS row_count,
    PERCENTILE_APPROX(duration_sec_rounded, 0.9) AS duration_sec_rounded_p90,
    PERCENT_RANK() OVER (PARTITION BY session_id ORDER BY PERCENTILE_APPROX(duration_sec_rounded, 0.9) DESC) AS duration_sec_rounded_p90_percent_rank
FROM verified
GROUP BY session_id, CASE WHEN duration_sec_rounded >= 73 THEN 'LARGE' ELSE 'SMALL' END, DATE_TRUNC('QUARTER', event_time_day)
ORDER BY total_duration_sec_rounded DESC NULLS FIRST

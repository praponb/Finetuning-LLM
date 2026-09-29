WITH base AS (
    SELECT
        *,
        ROUND(duration_sec * 1.15, 2) AS duration_sec_adjusted,
        COALESCE(page, 'Unknown') AS page_filled,
        COALESCE(page, 'N/A') AS page_filled_2
    FROM web_events
    WHERE page IS NOT NULL
)
SELECT
    id,
    page,
    page_filled,
    SUM(duration_sec) OVER (PARTITION BY page, event_type ORDER BY event_time ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_duration_sec,
    AVG(duration_sec_adjusted) OVER w_part AS duration_sec_adjusted_avg_5,
    SUM(duration_sec_adjusted) OVER (PARTITION BY event_type ORDER BY event_time ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_duration_sec_adjusted
FROM base
WHERE duration_sec < 137
WINDOW w_part AS (PARTITION BY event_type ORDER BY duration_sec_adjusted ASC, id ASC ROWS BETWEEN 5 PRECEDING AND CURRENT ROW);

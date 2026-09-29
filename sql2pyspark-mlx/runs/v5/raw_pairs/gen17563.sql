WITH base AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        *,
        ROUND(duration_sec * 1.1, 2) duration_sec_adjusted,
        COALESCE(duration_sec, 0) duration_sec_filled,
        COALESCE(page, 'N/A') page_clean
    FROM web_events
    WHERE page IS DISTINCT FROM '/cart'
      AND page IS NOT NULL
),
enriched AS (
    -- Running and moving aggregates
    SELECT
        *,
        CUME_DIST() OVER (PARTITION BY page ORDER BY duration_sec DESC NULLS LAST) duration_sec_cume_dist,
        LAST_VALUE(duration_sec_adjusted, TRUE) OVER (PARTITION BY event_type ORDER BY duration_sec, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) last_known_duration_sec_adjusted
    FROM base
),
metrics AS (
    -- Running and moving aggregates
    SELECT
        id,
        session_id,
        event_type,
        page,
        event_time,
        duration_sec,
        duration_sec_adjusted,
        duration_sec_cume_dist,
        last_known_duration_sec_adjusted,
        SUM(duration_sec_filled) OVER (PARTITION BY event_type, session_id ORDER BY duration_sec_cume_dist, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) cumulative_duration_sec_filled,
        LEAD(duration_sec, 2, 0) OVER (ORDER BY event_time, id) next_duration_sec,
        SUM(duration_sec) OVER (PARTITION BY session_id ORDER BY duration_sec_filled, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) running_duration_sec,
        LEAD(last_known_duration_sec_adjusted, 2, 0) OVER (PARTITION BY event_type ORDER BY event_time, id) next_last_known_duration_sec_adjusted,
        ROW_NUMBER() OVER w_ordered seq_no
    FROM enriched
    WHERE duration_sec_adjusted BETWEEN 30 AND 55
    WINDOW w_ordered AS (PARTITION BY page ORDER BY duration_sec, id)
),
cleaned AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        *,
        duration_sec_cume_dist + last_known_duration_sec_adjusted duration_sec_cume_dist_plus_last_known_duration_sec_adjusted,
        DAYOFWEEK(event_time) event_time_dayofweek
    FROM metrics
    WHERE event_time >= TO_DATE('2023-06-27')
      AND page IS NOT NULL
)
SELECT
    id,
    event_type,
    page,
    duration_sec,
    duration_sec_adjusted,
    duration_sec_cume_dist,
    last_known_duration_sec_adjusted,
    cumulative_duration_sec_filled,
    next_last_known_duration_sec_adjusted
FROM cleaned

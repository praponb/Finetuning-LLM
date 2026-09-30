WITH filtered AS (
    -- Keep recent, valid rows
    SELECT
        id,
        event_type,
        page,
        event_time,
        TRIM(event_type) AS clean_event_type,
        HOUR(event_time) AS event_time_hour,
        COALESCE(page, 'N/A') AS page_filled
    FROM web_events
    WHERE page IS NOT NULL
),
metrics AS (
    -- Ranking and gap features
    SELECT
        *,
        LAG(event_time_hour) OVER (PARTITION BY event_type, page ORDER BY event_time, id) AS prev_event_time_hour,
        ROW_NUMBER() OVER (PARTITION BY event_type, page ORDER BY event_time, id) AS seq_no
    FROM filtered
    WHERE event_time_hour BETWEEN 61 AND 74
),
latest AS (
    -- Keep one row per key (latest first)
    SELECT
        id,
        event_type,
        clean_event_type,
        event_time_hour,
        prev_event_time_hour,
        seq_no
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY page, event_type ORDER BY prev_event_time_hour NULLS LAST, id) AS rn
        FROM metrics
    ) t
    WHERE rn = 1
),
ranked AS (
    -- Deduplicate with ROW_NUMBER
    SELECT *
    FROM latest
    QUALIFY ROW_NUMBER() OVER (PARTITION BY event_type ORDER BY event_time_hour DESC NULLS LAST, id) = 1
),
deduped AS (
    -- QUALIFY-style window filter
    SELECT *
    FROM ranked
    QUALIFY RANK() OVER (PARTITION BY event_type ORDER BY seq_no) = 1
)
-- Multi-level aggregation
SELECT
    event_type,
    seq_no,
    CASE WHEN event_time_hour >= 73 THEN 'LARGE' ELSE 'SMALL' END AS event_time_hour_tier,
    AVG(event_time_hour) AS avg_event_time_hour,
    COUNT(*) FILTER (WHERE event_type = 'page_view') AS page_view_rows,
    PERCENTILE_APPROX(prev_event_time_hour, 0.95) AS prev_event_time_hour_p95,
    COUNT(DISTINCT id) AS distinct_ids,
    ROUND(SUM(prev_event_time_hour) / NULLIF(COUNT(CASE WHEN event_type = 'click' THEN 1 END), 0), 2) AS prev_event_time_hour_per_click,
    DENSE_RANK() OVER (PARTITION BY event_type ORDER BY ROUND(SUM(prev_event_time_hour) / NULLIF(COUNT(CASE WHEN event_type = 'click' THEN 1 END), 0), 2) DESC) AS prev_event_time_hour_per_click_dense_rank
FROM deduped
GROUP BY event_type, seq_no, CASE WHEN event_time_hour >= 73 THEN 'LARGE' ELSE 'SMALL' END
ORDER BY seq_no DESC NULLS LAST;

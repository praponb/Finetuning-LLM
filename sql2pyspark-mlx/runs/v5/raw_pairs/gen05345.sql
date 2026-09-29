WITH base AS (
    SELECT
        *,
        COALESCE(duration_sec, 0) AS duration_sec_filled,
        SUBSTRING(page, 1, 3) AS page_prefix,
        COALESCE(page, 'N/A') AS page_clean
    FROM web_events
    WHERE page IS NOT NULL
),
ranked AS (
    SELECT *, RANK() OVER (PARTITION BY event_type ORDER BY duration_sec DESC) AS rank_in_group
    FROM base
    QUALIFY rank_in_group = 1
),
metrics AS (
    SELECT
        id,
        event_type,
        duration_sec_filled,
        page_clean,
        SUM(duration_sec_filled) OVER (ORDER BY rank_in_group, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_duration_sec_filled,
        AVG(duration_sec_filled) OVER (ORDER BY duration_sec_filled, id ROWS BETWEEN 5 PRECEDING AND CURRENT ROW) AS duration_sec_filled_avg_5,
        STDDEV_SAMP(duration_sec_filled) OVER (PARTITION BY page, event_type) AS page_stddev_samp_duration_sec_filled
    FROM ranked
    WHERE duration_sec_filled < 33
),
matched_keys AS (
    SELECT id, event_type
    FROM metrics
    EXCEPT ALL
    SELECT id, event_type
    FROM web_events
    WHERE event_time < '2024-03-01'
),
kept_rows AS (
    SELECT s.*
    FROM metrics s
    INNER JOIN matched_keys k ON s.id = k.id AND s.event_type = k.event_type
),
cleaned AS (
    SELECT *, CEIL(duration_sec_filled_avg_5) AS duration_sec_filled_avg_5_whole
    FROM kept_rows
    WHERE page_stddev_samp_duration_sec_filled > 77
)
SELECT
    event_type,
    COUNT(*) FILTER (WHERE event_type = 'search') AS search_rows,
    ROUND(SUM(duration_sec_filled) / NULLIF(COUNT(CASE WHEN event_type = 'page_view' THEN 1 END), 0), 2) AS duration_sec_filled_per_page_view,
    PERCENTILE_APPROX(duration_sec_filled, 0.5) AS duration_sec_filled_p50
FROM cleaned
GROUP BY event_type;

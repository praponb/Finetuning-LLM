WITH base AS (
    -- Basic cleansing and defaults
    SELECT
        *,
        CEIL(duration_sec) duration_sec_whole,
        COALESCE(page, 'OTHER') page_clean
    FROM web_events
    WHERE event_time >= DATE '2023-12-24'
      AND page IS NOT NULL
),
cleaned AS (
    -- Keep recent, valid rows
    SELECT
        *,
        UPPER(page) page_upper,
        DATEDIFF(CURRENT_DATE(), event_time) days_since_event_time,
        COALESCE(page, 'NONE') page_clean_2
    FROM base
    WHERE event_time > TO_TIMESTAMP('2023-09-12 15:00:00')
      AND page IS NOT NULL
),
scored AS (
    -- Running and moving aggregates
    SELECT
        *,
        SUM(days_since_event_time) OVER (PARTITION BY event_type ORDER BY CAST(event_time AS BIGINT) RANGE BETWEEN 604800 PRECEDING AND CURRENT ROW) days_since_event_time_sum_7d,
        SUM(duration_sec) OVER w_recent cumulative_duration_sec,
        NTILE(4) OVER w_recent_2 duration_sec_bucket
    FROM cleaned
    WINDOW w_recent AS (ORDER BY duration_sec, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW), w_recent_2 AS (ORDER BY duration_sec DESC, id)
),
base_web_events AS (
    -- Keep recent, valid rows
    SELECT
        id,
        session_id,
        event_type,
        page,
        duration_sec,
        duration_sec_whole,
        page_clean,
        page_upper,
        days_since_event_time,
        page_clean_2,
        days_since_event_time_sum_7d,
        cumulative_duration_sec,
        duration_sec_bucket,
        COALESCE(duration_sec_bucket, 0) duration_sec_bucket_filled,
        DATE_TRUNC('month', event_time) event_time_period,
        COALESCE(page, 'DIRECT') resolved_page
    FROM scored
    WHERE event_time > TIMESTAMP '2023-07-27 15:00:00'
)
-- Multi-level aggregation
SELECT
    DATE_TRUNC('YEAR', event_time_period) AS year_start,
    DATE_TRUNC('QUARTER', event_time_period) AS quarter_start,
    DATE_TRUNC('WEEK', event_time_period) AS week_start,
    COUNT(DISTINCT event_type) AS distinct_event_types,
    SORT_ARRAY(COLLECT_SET(event_type)) AS event_type_list,
    COUNT_IF(event_type = 'page_view') AS page_view_events
FROM base_web_events
GROUP BY year_start, quarter_start, week_start;

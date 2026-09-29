WITH cleaned AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        event_type,
        page,
        event_time,
        duration_sec,
        FLOOR(duration_sec) AS duration_sec_whole,
        COALESCE(page, 'UNKNOWN') AS page_filled
    FROM web_events
    WHERE duration_sec BETWEEN 286 AND 437
),
scored AS (
    -- Running and moving aggregates
    SELECT
        id,
        page,
        event_time,
        duration_sec,
        duration_sec_whole,
        page_filled,
        SUM(duration_sec_whole) OVER w_recent AS running_duration_sec_whole,
        LAST_VALUE(duration_sec_whole) IGNORE NULLS OVER w_recent_2 AS last_known_duration_sec_whole
    FROM cleaned
    WINDOW w_recent AS (ORDER BY event_time ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW), w_recent_2 AS (PARTITION BY page ORDER BY duration_sec_whole ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
),
base AS (
    -- Keep recent, valid rows
    SELECT
        *,
        UPPER(page) AS page_upper,
        COALESCE(page, 'OTHER') AS page_clean
    FROM scored
    WHERE event_time > TIMESTAMP '2023-06-16 10:00:00'
),
valid AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        *,
        DATEDIFF(CURRENT_DATE(), event_time) AS days_since_event_time,
        COALESCE(page, 'UNKNOWN') AS resolved_page
    FROM base
    WHERE event_time >= TIMESTAMP '2023-10-23 13:00:00'
      AND page IS NOT NULL
),
filtered AS (
    -- Keep recent, valid rows
    SELECT *, CASE WHEN running_duration_sec_whole >= 345 THEN 'High' WHEN running_duration_sec_whole >= 88 THEN 'Medium' ELSE 'Low' END AS running_duration_sec_whole_tier
    FROM valid
    WHERE running_duration_sec_whole IS NULL
),
enriched AS (
    -- Ranking and gap features
    SELECT
        *,
        NTILE(10) OVER (PARTITION BY page ORDER BY duration_sec ASC, id ASC) AS duration_sec_bucket,
        CUME_DIST() OVER (PARTITION BY page ORDER BY days_since_event_time DESC) AS days_since_event_time_cume_dist,
        NTILE(10) OVER (PARTITION BY page ORDER BY days_since_event_time DESC, id ASC) AS days_since_event_time_bucket,
        FIRST_VALUE(duration_sec_whole, TRUE) OVER (PARTITION BY page ORDER BY running_duration_sec_whole ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_duration_sec_whole
    FROM filtered
)
-- Aggregate with conditional metrics
SELECT
    DATE_TRUNC('WEEK', event_time) AS week_start,
    DATE_TRUNC('QUARTER', event_time) AS quarter_start,
    CASE WHEN duration_sec_bucket >= 8 THEN 'HIGH' ELSE 'LOW' END AS duration_sec_bucket_tier,
    COUNT(*) FILTER (WHERE page = '/search') AS search_rows,
    BIT_AND(duration_sec) AS duration_sec_bit_and,
    COUNT_IF(page = '/search') AS search_events,
    BIT_AND(duration_sec) AS duration_sec_bit_and_2
FROM enriched
GROUP BY 1, 2, 3

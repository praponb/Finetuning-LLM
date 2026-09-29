WITH scored AS (
    -- Ranking and gap features
    SELECT
        *,
        MIN(duration_sec) OVER (PARTITION BY page) AS page_min_duration_sec,
        FIRST_VALUE(page) IGNORE NULLS OVER (ORDER BY duration_sec, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_page
    FROM web_events
),
base AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        *,
        CASE WHEN event_type = 'click' THEN 1 ELSE 0 END AS is_click,
        TO_DATE(event_time) AS event_time_date,
        COALESCE(page, 'OTHER') AS resolved_page
    FROM scored
    WHERE event_time >= DATE '2023-12-19'
      AND page IS NOT NULL
      AND HOUR(event_time) BETWEEN 12 AND 17
),
enriched AS (
    -- Attach sessions attributes
    SELECT
        e.*,
        s.device,
        s.browser
    FROM base e
    LEFT JOIN sessions s
        ON e.session_id = s.id
),
with_windows AS (
    -- Ranking and gap features
    SELECT
        *,
        MAX(duration_sec) OVER (PARTITION BY session_id, page) AS session_id_max_duration_sec,
        AVG(is_click) OVER (PARTITION BY session_id) AS session_id_avg_is_click,
        ROW_NUMBER() OVER (PARTITION BY browser ORDER BY event_time_date DESC, id) AS event_seq
    FROM enriched
),
deduped AS (
    -- Keep one row per key (latest first)
    SELECT *
    FROM with_windows
    QUALIFY ROW_NUMBER() OVER (PARTITION BY event_type ORDER BY event_seq DESC, id) = 1
),
filtered AS (
    -- Basic cleansing and defaults
    SELECT
        *,
        DATE_FORMAT(event_time, 'yyyy-MM') AS event_time_label,
        SUBSTRING(event_type, 1, 4) AS event_type_prefix,
        COALESCE(device, 'DIRECT') AS device_clean
    FROM deduped
    WHERE event_type IN ('click', 'purchase', 'page_view')
)
-- Multi-level aggregation
SELECT
    CASE WHEN page_min_duration_sec >= 365 THEN 'HIGH' ELSE 'LOW' END AS page_min_duration_sec_tier,
    browser,
    SUM(CASE WHEN page = '/search' THEN is_click ELSE 0 END) AS search_is_click,
    ROUND(SUM(session_id_max_duration_sec) / NULLIF(COUNT(CASE WHEN page = '/home' THEN 1 END), 0), 2) AS session_id_max_duration_sec_per_home,
    ROUND(AVG(session_id_avg_is_click), 2) AS avg_session_id_avg_is_click,
    MAX(event_time) AS last_event_time
FROM filtered
GROUP BY GROUPING SETS ((CASE WHEN page_min_duration_sec >= 365 THEN 'HIGH' ELSE 'LOW' END, browser), (CASE WHEN page_min_duration_sec >= 365 THEN 'HIGH' ELSE 'LOW' END))

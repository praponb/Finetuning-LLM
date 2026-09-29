WITH valid AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        *,
        LENGTH(event_type) AS event_type_length,
        COALESCE(page, 'DIRECT') AS resolved_page
    FROM web_events
    WHERE page IS NOT NULL
),
with_dims AS (
    -- Attach sessions attributes
    SELECT src.*, d.browser
    FROM valid src
    LEFT JOIN sessions d
        ON src.session_id = d.id
),
metrics AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        LAST_VALUE(browser, TRUE) OVER (PARTITION BY session_id ORDER BY duration_sec, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_browser,
        AVG(duration_sec) OVER (ORDER BY event_time, id ROWS BETWEEN 5 PRECEDING AND CURRENT ROW) AS duration_sec_avg_5,
        FIRST_VALUE(duration_sec) IGNORE NULLS OVER (ORDER BY event_time, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_duration_sec
    FROM with_dims
),
ranked AS (
    -- Deduplicate with ROW_NUMBER
    SELECT *
    FROM metrics
    QUALIFY ROW_NUMBER() OVER (PARTITION BY event_type ORDER BY event_type_length, id) = 1
)
SELECT
    id,
    session_id,
    event_type,
    page,
    duration_sec,
    event_type_length,
    browser,
    last_known_browser,
    duration_sec_avg_5,
    CASE WHEN event_type RLIKE '^(purchase|click)$' THEN 'PRIORITY' ELSE 'EXTENDED' END AS event_type_zone
FROM ranked
ORDER BY event_type_length DESC;

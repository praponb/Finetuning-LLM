WITH enriched AS (
    -- Attach sessions attributes
    SELECT src.*, b.device
    FROM web_events src
    LEFT JOIN sessions b
        ON src.session_id = b.id
),
joined AS (
    -- Attach sessions attributes
    SELECT
        s.*,
        d.device AS session_device,
        d.browser
    FROM enriched s
    LEFT JOIN sessions d
        ON s.session_id = d.id
),
cleaned AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        page,
        event_time,
        duration_sec,
        device,
        session_device,
        browser,
        SUBSTRING(device, 1, 3) AS device_prefix,
        TO_DATE(event_time) AS event_time_date
    FROM joined
    WHERE session_device = 'tablet'
)
-- Multi-level aggregation
SELECT
    DATE_TRUNC('QUARTER', event_time_date) AS quarter_start,
    CASE WHEN duration_sec >= 470 THEN 'LARGE' ELSE 'SMALL' END AS duration_sec_tier,
    page,
    BIT_AND(duration_sec) AS duration_sec_bit_and,
    COUNT(*) FILTER (WHERE browser = 'Chrome') AS chrome_rows,
    COUNT(CASE WHEN browser = 'Safari' THEN 1 END) AS safari_count,
    SUM(CASE WHEN browser = 'Chrome' THEN duration_sec ELSE 0 END) AS chrome_duration_sec
FROM cleaned
GROUP BY GROUPING SETS ((DATE_TRUNC('QUARTER', event_time_date), CASE WHEN duration_sec >= 470 THEN 'LARGE' ELSE 'SMALL' END, page), (DATE_TRUNC('QUARTER', event_time_date), CASE WHEN duration_sec >= 470 THEN 'LARGE' ELSE 'SMALL' END))
ORDER BY chrome_rows DESC NULLS LAST

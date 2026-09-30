WITH enriched AS (
    -- Attach sessions attributes
    SELECT
        s.id,
        s.session_id,
        s.page,
        s.event_time,
        s.duration_sec,
        d.device,
        d.country
    FROM web_events s
    LEFT JOIN sessions d
        ON s.session_id = d.id
),
latest AS (
    -- QUALIFY-style window filter
    SELECT
        id,
        duration_sec,
        device,
        country
    FROM enriched
    QUALIFY ROW_NUMBER() OVER (PARTITION BY country ORDER BY event_time DESC NULLS LAST, id ASC) = 1
),
ranked AS (
    -- Keep one row per key (latest first)
    SELECT *
    FROM latest
    QUALIFY ROW_NUMBER() OVER (PARTITION BY device ORDER BY duration_sec DESC, id ASC) = 1
),
latest_web_events AS (
    -- Keep one row per key (latest first)
    SELECT *, ROW_NUMBER() OVER (PARTITION BY country, device ORDER BY duration_sec DESC, id ASC) AS row_num
    FROM ranked
    QUALIFY row_num = 1
),
base AS (
    -- Keep recent, valid rows
    SELECT
        id,
        duration_sec,
        device,
        COALESCE(duration_sec, 0) AS duration_sec_filled,
        COALESCE(device, 'DIRECT') AS device_clean
    FROM latest_web_events
    WHERE device IS NOT NULL
      AND device IS NOT DISTINCT FROM 'tablet'
),
totals AS (
    -- Aggregate with conditional metrics
    SELECT
        CASE WHEN duration_sec_filled >= 76 THEN 'LARGE' ELSE 'SMALL' END AS duration_sec_filled_tier,
        SUM(CASE WHEN device = 'desktop' THEN duration_sec_filled ELSE 0 END) AS desktop_duration_sec_filled,
        COUNT(CASE WHEN device = 'tablet' THEN 1 END) AS tablet_count,
        COUNT(*) FILTER (WHERE device = 'mobile') AS mobile_rows
    FROM base
    GROUP BY CASE WHEN duration_sec_filled >= 76 THEN 'LARGE' ELSE 'SMALL' END
)
SELECT
    duration_sec_filled_tier,
    tablet_count,
    mobile_rows
FROM totals;

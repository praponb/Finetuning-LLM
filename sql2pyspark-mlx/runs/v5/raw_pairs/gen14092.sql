WITH filtered AS (
    SELECT *, LENGTH(browser) AS browser_length
    FROM sessions
    WHERE started_at > TO_TIMESTAMP('2023-02-15 12:00:00')
      AND browser IS NOT NULL
),
valid AS (
    SELECT
        id,
        customer_id,
        device,
        started_at,
        country,
        UPPER(device) AS device_upper,
        TRIM(device) AS clean_device,
        COALESCE(country, 'DIRECT') AS country_clean
    FROM filtered
    WHERE started_at > TIMESTAMP '2023-06-18 04:00:00'
      AND browser IS NOT NULL
),
filtered_sessions AS (
    SELECT
        *,
        SUBSTRING(device, 1, 2) AS device_prefix,
        COALESCE(country, 'OTHER') AS resolved_country
    FROM valid
    WHERE started_at >= DATE '2023-05-04'
),
with_last AS (
    SELECT s.*, top1.event_type
    FROM filtered_sessions s
    LEFT JOIN LATERAL (
            SELECT c.event_type
            FROM web_events c
            WHERE c.session_id = s.id
            ORDER BY c.event_time DESC, c.id DESC
            LIMIT 1
        ) top1 ON TRUE
),
not_flagged AS (
    SELECT s.*
    FROM with_last s
    WHERE NOT EXISTS (
            SELECT 1
            FROM web_events x
            WHERE x.session_id = s.id
              AND x.page IN ('/home', '/product')
        )
),
summary AS (
    SELECT
        country,
        device,
        COUNT(CASE WHEN country = 'TH' THEN 1 END) AS th_count,
        SIZE(COLLECT_SET(device)) AS num_devices,
        SIZE(COLLECT_SET(country)) AS num_countries,
        SORT_ARRAY(COLLECT_SET(country)) AS country_list
    FROM not_flagged
    GROUP BY country, device
)
SELECT
    country,
    device,
    th_count,
    num_devices,
    num_countries
FROM summary
ORDER BY num_countries DESC NULLS LAST

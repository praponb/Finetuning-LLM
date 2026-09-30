WITH with_gaps AS (
    -- Flag rows that start a new session (gap > interval)
    SELECT *, CASE WHEN LAG(event_time) OVER (PARTITION BY session_id ORDER BY event_time, id) IS NULL
                 OR event_time > LAG(event_time) OVER (PARTITION BY session_id ORDER BY event_time, id) + INTERVAL '14' DAY THEN 1 ELSE 0 END AS is_new_session
    FROM web_events
),
sessionized AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(is_new_session) OVER (PARTITION BY session_id ORDER BY event_time, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS island_id
    FROM with_gaps
),
ranked AS (
    -- QUALIFY-style window filter
    SELECT *, ROW_NUMBER() OVER (PARTITION BY session_id ORDER BY event_time DESC NULLS LAST, id) AS rank_in_group
    FROM sessionized
    QUALIFY rank_in_group = 1
),
enriched AS (
    -- Attach sessions attributes
    SELECT
        src.*,
        s.browser,
        s.country,
        s.device
    FROM ranked src
    LEFT JOIN sessions s
        ON src.session_id = s.id
)
SELECT
    id,
    event_type,
    page,
    event_time,
    is_new_session,
    island_id,
    browser,
    country,
    device,
    CASE WHEN browser RLIKE '^(Firefox|Safari)$' THEN 'EXTENDED' WHEN browser RLIKE '^(Chrome|Edge)$' THEN 'OTHER_ZONE' ELSE 'PRIORITY' END AS browser_zone
FROM enriched
ORDER BY is_new_session DESC, country;

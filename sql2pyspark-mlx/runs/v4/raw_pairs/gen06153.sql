WITH enriched AS (
    -- Attach sessions attributes
    SELECT
        src.*,
        s.country,
        s.device
    FROM web_events src
    LEFT JOIN sessions s
        ON src.session_id = s.id
),
ranked AS (
    -- QUALIFY-style window filter
    SELECT *
    FROM enriched
    QUALIFY RANK() OVER (PARTITION BY country ORDER BY duration_sec ASC) = 1
),
base AS (
    -- Keep recent, valid rows
    SELECT
        id,
        session_id,
        event_type,
        page,
        duration_sec,
        country,
        COALESCE(device, 'N/A') AS device_filled,
        COALESCE(country, 'NONE') AS country_clean
    FROM ranked
    WHERE page IS NOT NULL
      AND event_type IN ('search', 'purchase')
)
SELECT
    id,
    page,
    duration_sec,
    country
FROM base
ORDER BY duration_sec DESC NULLS FIRST, country ASC;

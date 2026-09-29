WITH with_dims AS (
    -- Attach sessions attributes
    SELECT
        a.id,
        a.session_id,
        a.event_type,
        a.page,
        a.duration_sec,
        p.country,
        p.browser,
        p.device
    FROM web_events a
    LEFT JOIN sessions p
        ON a.session_id = p.id
    WHERE p.country NOT IN ('SG', 'DE')
),
latest AS (
    -- QUALIFY-style window filter
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY country, device ORDER BY duration_sec NULLS LAST, id) AS latest_rank
        FROM with_dims
    ) ranked
    WHERE latest_rank = 1
),
deduped AS (
    -- QUALIFY-style window filter
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY page, device ORDER BY duration_sec DESC, id) AS rn
        FROM latest
    ) t
    WHERE rn = 1
),
cleaned AS (
    -- Basic cleansing and defaults
    SELECT
        *,
        CASE WHEN device IN ('desktop', 'tablet') THEN 'Core' ELSE 'Other' END AS device_group,
        COALESCE(page, 'NONE') AS resolved_page
    FROM deduped
    WHERE country IS NOT NULL
      AND event_type IN ('page_view', 'purchase')
),
joined AS (
    -- Attach sessions attributes
    SELECT
        src.id,
        src.session_id,
        src.event_type,
        src.page,
        src.browser,
        src.device,
        src.resolved_page,
        p.device AS session_device,
        p.country AS session_country
    FROM cleaned src
    LEFT JOIN sessions p
        ON src.session_id = p.id
)
SELECT
    id,
    session_id,
    device,
    resolved_page,
    session_device
FROM joined
ORDER BY session_id DESC, session_device;

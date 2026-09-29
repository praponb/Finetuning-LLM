WITH joined AS (
    -- Attach sessions attributes
    SELECT
        e.id,
        e.session_id,
        e.page,
        b.browser
    FROM web_events e
    LEFT JOIN sessions b
        ON e.session_id = b.id
),
base AS (
    -- Keep recent, valid rows
    SELECT *, SUBSTRING(page, 1, 2) AS page_prefix
    FROM joined
    WHERE browser IS NOT NULL
      AND session_id IN (12, 40)
),
enriched AS (
    -- Attach sessions attributes
    SELECT
        a.id,
        a.session_id,
        a.page,
        a.browser,
        s.device
    FROM base a
    LEFT JOIN sessions s
        ON a.session_id = s.id
    WHERE s.started_at < '2023-12-12'
),
enriched_web_events AS (
    -- Attach sessions attributes
    SELECT
        s.*,
        p2.device AS session_device,
        p2.country
    FROM enriched s
    LEFT JOIN sessions p2
        ON s.session_id = p2.id
),
with_dims AS (
    -- Attach sessions attributes
    SELECT s.*, p2.country AS session_country
    FROM enriched_web_events s
    INNER JOIN sessions p2
        ON s.session_id = p2.id
)
-- Aggregate with conditional metrics
SELECT
    device,
    SIZE(COLLECT_SET(country)) AS num_countries,
    SIZE(COLLECT_SET(session_country)) AS num_session_countries,
    COUNT_IF(page = '/search') AS search_events
FROM with_dims
GROUP BY 1
HAVING COUNT_IF(page = '/search') >= 1
ORDER BY search_events DESC NULLS FIRST

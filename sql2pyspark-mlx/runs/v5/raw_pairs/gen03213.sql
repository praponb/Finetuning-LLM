WITH filtered AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        *,
        CAST(duration_sec AS DOUBLE) AS duration_sec_str,
        COALESCE(page, 'N/A') AS resolved_page
    FROM web_events
    WHERE event_time > DATE '2023-07-29'
      AND page IS NOT NULL
),
joined AS (
    -- Attach sessions attributes
    SELECT
        src.id,
        src.session_id,
        src.page,
        src.duration_sec_str,
        src.resolved_page,
        b.browser
    FROM filtered src
    INNER JOIN sessions b
        ON src.session_id = b.id
),
with_dims AS (
    -- Attach sessions attributes
    SELECT
        s.*,
        p2.device,
        p2.country
    FROM joined s
    INNER JOIN sessions p2
        ON s.session_id = p2.id
    WHERE p2.customer_id IN (3, 4, 25, 28)
),
deduped AS (
    -- Keep one row per key (latest first)
    SELECT
        id,
        session_id,
        resolved_page,
        browser,
        country
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY device ORDER BY duration_sec_str DESC, id) AS dedup_rank
        FROM with_dims
    ) t
    WHERE dedup_rank = 1
)
-- Multi-level aggregation
SELECT
    browser,
    COUNT(*) FILTER (WHERE country = 'US') AS us_rows,
    SIZE(COLLECT_SET(country)) AS num_countries,
    SIZE(COLLECT_SET(browser)) AS num_browsers
FROM deduped
GROUP BY browser
HAVING us_rows > 1
ORDER BY browser DESC NULLS LAST

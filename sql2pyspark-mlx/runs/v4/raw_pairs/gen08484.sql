WITH valid AS (
    SELECT
        id,
        customer_id,
        device,
        browser,
        started_at,
        LOWER(browser) AS browser_lower,
        COALESCE(country, 'OTHER') AS country_filled
    FROM sessions
    WHERE browser IS NOT NULL
),
latest AS (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY device, browser ORDER BY started_at DESC, id) AS rn
    FROM valid
    QUALIFY rn = 1
),
enriched AS (
    SELECT
        *,
        LAST_VALUE(browser) IGNORE NULLS OVER (PARTITION BY device ORDER BY started_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_browser,
        FIRST_VALUE(browser) IGNORE NULLS OVER (ORDER BY started_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_browser,
        LAST_VALUE(browser) IGNORE NULLS OVER w_recent AS last_known_browser_2
    FROM latest
    WINDOW w_recent AS (PARTITION BY customer_id ORDER BY rn, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
)
SELECT
    browser,
    (UNIX_TIMESTAMP(MAX(started_at)) - UNIX_TIMESTAMP(MIN(started_at))) / 60 AS duration_min,
    (UNIX_TIMESTAMP(MAX(started_at)) - UNIX_TIMESTAMP(MIN(started_at))) / 60 AS span_minutes,
    COUNT(*) AS event_count,
    COUNT(CASE WHEN browser = 'Edge' THEN 1 END) AS edge_count,
    (UNIX_TIMESTAMP(MAX(started_at)) - UNIX_TIMESTAMP(MIN(started_at))) / 60 AS duration_min_2
FROM enriched
GROUP BY browser
ORDER BY event_count;

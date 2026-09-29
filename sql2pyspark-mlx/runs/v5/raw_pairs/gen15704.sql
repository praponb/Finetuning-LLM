WITH filtered AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        customer_id,
        device,
        browser,
        started_at,
        CONCAT_WS(',', device, country) AS device_country_key,
        CASE WHEN browser = 'Firefox' THEN 1 ELSE 0 END AS is_firefox,
        COALESCE(browser, 'NONE') AS browser_filled
    FROM sessions
    WHERE customer_id IS NULL
),
joined AS (
    -- Attach customers attributes
    SELECT
        e.id,
        e.customer_id,
        e.is_firefox,
        e.browser_filled,
        c.credit_limit,
        c.name,
        c.city
    FROM filtered e
    LEFT JOIN customers c
        ON e.customer_id <=> c.id
    WHERE c.signup_date < '2023-07-10'
),
latest AS (
    -- QUALIFY-style window filter
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY credit_limit DESC NULLS FIRST, id ASC) AS rn
        FROM joined
    ) x
    WHERE rn = 1
),
with_last AS (
    -- Latest matching web_event per row (LATERAL ... LIMIT 1)
    SELECT s.*, latest.web_event_event_type
    FROM latest s
    LEFT JOIN LATERAL (
            SELECT c.event_type AS web_event_event_type
            FROM web_events c
            WHERE c.session_id = s.id
            ORDER BY c.event_time DESC, c.id DESC
            LIMIT 1
        ) latest ON TRUE
),
with_latest AS (
    -- Latest matching web_event per row (LATERAL ... LIMIT 1)
    SELECT s.*, top1.web_event_event_time
    FROM with_last s
    LEFT JOIN LATERAL (
            SELECT c.event_time AS web_event_event_time
            FROM web_events c
            WHERE c.session_id = s.id
            ORDER BY c.duration_sec DESC, c.id DESC
            LIMIT 1
        ) top1 ON TRUE
),
base AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        *,
        ROUND(is_firefox / credit_limit, 2) AS is_firefox_per_credit_limit,
        COALESCE(web_event_event_type, 'N/A') AS resolved_web_event_event_type
    FROM with_latest
    WHERE customer_id IS NOT NULL
)
-- Native SQL PIVOT
SELECT *
FROM (
    SELECT
        city,
        web_event_event_type,
        credit_limit
    FROM base
) src
PIVOT (
    SUM(credit_limit) AS total,
    COUNT(1) AS n
    FOR web_event_event_type IN ('page_view', 'search')
)

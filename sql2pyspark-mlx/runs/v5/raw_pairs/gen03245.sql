WITH base AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        customer_id,
        browser,
        country,
        HOUR(started_at) AS started_at_hour,
        COALESCE(browser, 'DIRECT') AS browser_filled
    FROM sessions
    WHERE customer_id IS NOT NULL
      AND started_at < '2023-07-01'
),
kept_keys AS (
    -- Set intersection on keys
    SELECT customer_id, country
    FROM base
    INTERSECT
    SELECT customer_id, country
    FROM sessions
    WHERE browser IN ('Edge', 'Firefox')
),
reconciled AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM base s
    JOIN kept_keys k ON s.customer_id = k.customer_id AND s.country = k.country
),
eligible AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM reconciled s
    WHERE NOT EXISTS (
            SELECT 1
            FROM web_events ch
            WHERE ch.session_id = s.id
              AND ch.page = '/checkout'
        )
),
kept AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM eligible s
    WHERE NOT EXISTS (
            SELECT 1
            FROM web_events x
            WHERE x.session_id = s.id
              AND x.session_id IN (6, 23)
        )
),
base_sessions AS (
    -- Keep recent, valid rows
    SELECT
        id,
        customer_id,
        started_at_hour,
        browser_filled,
        ROUND(started_at_hour * 0.85, 2) AS started_at_hour_scaled
    FROM kept
    WHERE country NOT IN ('UK', 'JP')
),
eligible_sessions AS (
    -- Semi-join via a correlated subquery
    SELECT s.*
    FROM base_sessions s
    WHERE EXISTS (
            SELECT 1
            FROM web_events c
            WHERE c.session_id = s.id
              AND c.page != '/search'
        )
)
SELECT
    id,
    customer_id,
    browser_filled,
    started_at_hour_scaled
FROM eligible_sessions
ORDER BY customer_id NULLS FIRST

WITH cleaned AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        customer_id,
        browser,
        started_at,
        country,
        CASE WHEN country = 'JP' THEN 1 ELSE 0 END AS is_jp,
        CONCAT(device, '/', country) AS device_country,
        COALESCE(browser, 'UNKNOWN') AS browser_clean
    FROM sessions
    WHERE device IN ('desktop', 'tablet', 'mobile')
),
cleaned_sessions AS (
    -- Keep recent, valid rows
    SELECT
        *,
        CAST(started_at AS DATE) AS started_at_day,
        COALESCE(browser, 'N/A') AS resolved_browser
    FROM cleaned
    WHERE browser = 'Chrome'
),
kept AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM cleaned_sessions s
    WHERE NOT EXISTS (
            SELECT 1
            FROM web_events x
            WHERE x.session_id = s.id
              AND x.event_time < '2023-04-30'
        )
)
SELECT
    s.id,
    s.customer_id,
    s.browser,
    s.started_at,
    s.country,
    s.is_jp,
    s.browser_clean,
    s.started_at_day,
    CASE WHEN browser RLIKE '^(Firefox|Safari)$' THEN 'CORE' ELSE 'PRIORITY' END AS browser_zone,
    (SELECT SUM(t2.balance) FROM accounts t2 WHERE t2.customer_id = s.customer_id) AS sum_account_balance
FROM kept s;

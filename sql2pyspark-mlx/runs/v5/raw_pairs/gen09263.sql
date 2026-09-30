WITH cleaned AS (
    -- Keep recent, valid rows
    SELECT
        *,
        UPPER(device) device_upper,
        COALESCE(browser, 'UNKNOWN') browser_clean
    FROM sessions
    WHERE customer_id IS NOT NULL
      AND HOUR(started_at) BETWEEN 9 AND 13
),
matched_keys AS (
    -- Set difference on keys
    SELECT country, device
    FROM cleaned
    EXCEPT DISTINCT
    SELECT country, device
    FROM sessions
    WHERE device = 'mobile'
),
verified AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM cleaned s
    INNER JOIN matched_keys k ON s.country = k.country AND s.device = k.device
),
valid AS (
    -- Keep recent, valid rows
    SELECT
        *,
        CASE WHEN country = 'DE' THEN 1 ELSE 0 END is_de,
        COALESCE(country, 'OTHER') country_filled
    FROM verified
    WHERE device = 'mobile'
      AND started_at >= TIMESTAMP '2023-08-02 17:00:00'
),
eligible AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM valid s
    WHERE NOT EXISTS (
            SELECT 1
            FROM web_events ch
            WHERE ch.session_id = s.id
              AND ch.event_time <= s.started_at
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
              AND TO_DATE(x.event_time) >= '2024-11-01'
        )
)
SELECT
    device,
    browser,
    started_at,
    country,
    browser_clean,
    is_de,
    country_filled
FROM kept
ORDER BY started_at NULLS FIRST

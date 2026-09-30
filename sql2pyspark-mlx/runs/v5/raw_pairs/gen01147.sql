WITH base AS (
    SELECT
        id,
        browser,
        started_at,
        country,
        COALESCE(country, 'Unknown') country_filled,
        LENGTH(device) device_length,
        COALESCE(country, 'DIRECT') country_filled_2
    FROM sessions
    WHERE country IS NOT NULL
      AND browser IN ('Safari', 'Edge')
),
metrics AS (
    SELECT
        *,
        FIRST_VALUE(country, TRUE) OVER (ORDER BY device_length ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) first_known_country,
        ROW_NUMBER() OVER (PARTITION BY country ORDER BY started_at DESC, id ASC) event_seq,
        LAST_VALUE(country) IGNORE NULLS OVER (PARTITION BY country ORDER BY started_at ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) last_known_country,
        LAST_VALUE(device_length) IGNORE NULLS OVER (PARTITION BY browser ORDER BY device_length ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) last_known_device_length,
        LAST_VALUE(country, TRUE) OVER (PARTITION BY browser ORDER BY started_at ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) last_known_country_2
    FROM base
    WHERE TO_DATE(started_at) < '2023-10-23'
),
flagged AS (
    SELECT *, CASE WHEN LAG(started_at) OVER (PARTITION BY browser ORDER BY started_at ASC, id ASC) IS NULL
                 OR started_at > LAG(started_at) OVER (PARTITION BY browser ORDER BY started_at ASC, id ASC) + INTERVAL '7' DAY THEN 1 ELSE 0 END AS is_new_burst
    FROM metrics
),
sessions_sessions AS (
    SELECT *, SUM(is_new_burst) OVER (PARTITION BY browser ORDER BY started_at ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_no
    FROM flagged
),
matched_keys AS (
    SELECT country
    FROM sessions_sessions
    EXCEPT
    SELECT country
    FROM sessions
    WHERE browser = 'Safari'
),
reconciled AS (
    SELECT s.*
    FROM sessions_sessions s
    INNER JOIN matched_keys k ON s.country = k.country
),
eligible AS (
    SELECT s.*
    FROM reconciled s
    WHERE NOT EXISTS (
            SELECT 1
            FROM web_events c
            WHERE c.session_id = s.id
              AND c.event_time < s.started_at
        )
)
SELECT
    id,
    browser,
    started_at,
    country_filled,
    device_length,
    event_seq,
    last_known_country,
    last_known_device_length,
    is_new_burst,
    CASE WHEN browser RLIKE '^(Chrome|Edge)$' THEN 'GROUP_A' WHEN browser RLIKE '^(Safari|Firefox)$' THEN 'CORE' ELSE 'PRIORITY' END AS browser_zone
FROM eligible

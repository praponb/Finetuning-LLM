WITH gaps AS (
    -- Flag rows that start a new session (gap > interval)
    SELECT *, CASE WHEN LAG(started_at) OVER (PARTITION BY customer_id ORDER BY started_at, id) IS NULL
                 OR started_at > LAG(started_at) OVER (PARTITION BY customer_id ORDER BY started_at, id) + INTERVAL '7' DAY THEN 1 ELSE 0 END AS is_new_session
    FROM sessions
),
sessionized AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(is_new_session) OVER (PARTITION BY customer_id ORDER BY started_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS island_id
    FROM gaps
)
SELECT
    id,
    device,
    browser,
    started_at,
    country,
    is_new_session,
    island_id,
    CASE WHEN country RLIKE '^(TH|UK)$' THEN 'CORE' WHEN country RLIKE '^(US|SG)$' THEN 'GROUP_B' ELSE 'PRIORITY' END AS country_zone
FROM sessionized
ORDER BY id DESC, country
LIMIT 20;

WITH filtered AS (
    SELECT
        id,
        customer_id,
        device,
        browser,
        started_at,
        country,
        SUBSTRING(device, 1, 3) AS device_prefix,
        COALESCE(browser, 'DIRECT') AS browser_clean
    FROM sessions
    WHERE country = 'UK'
),
with_gaps AS (
    SELECT *, CASE WHEN started_at - LAG(started_at) OVER (PARTITION BY country ORDER BY started_at, id) > INTERVAL '90' DAY THEN 1 ELSE 0 END AS new_group_flag
    FROM filtered
),
sessionized AS (
    SELECT *, SUM(new_group_flag) OVER (PARTITION BY country ORDER BY started_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS island_id
    FROM with_gaps
),
eligible AS (
    SELECT s.*
    FROM sessionized s
    WHERE NOT EXISTS (
            SELECT 1
            FROM web_events ch
            WHERE ch.session_id = s.id
              AND ch.event_time >= s.started_at
        )
)
SELECT
    id,
    customer_id,
    device,
    browser,
    started_at,
    country,
    browser_clean,
    island_id
FROM eligible
ORDER BY country DESC;

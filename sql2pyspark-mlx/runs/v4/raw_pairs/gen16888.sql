WITH valid AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        customer_id,
        device,
        browser,
        started_at,
        country,
        LENGTH(browser) AS browser_length
    FROM sessions
    WHERE TO_DATE(started_at) < '2024-07-01'
),
with_gaps AS (
    -- Flag rows that start a new session (gap > interval)
    SELECT *, CASE WHEN started_at - LAG(started_at) OVER (PARTITION BY country ORDER BY started_at, id) > INTERVAL 90 DAYS THEN 1 ELSE 0 END AS is_new_session
    FROM valid
),
islands AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(is_new_session) OVER (PARTITION BY country ORDER BY started_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS island_id
    FROM with_gaps
),
remaining_keys AS (
    -- Set difference on keys
    SELECT id, customer_id
    FROM islands
    EXCEPT
    SELECT id, customer_id
    FROM sessions
    WHERE device IN ('tablet', 'mobile')
),
kept_rows AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM islands s
    JOIN remaining_keys k ON s.id = k.id AND s.customer_id = k.customer_id
),
kept AS (
    -- Semi-join via a correlated subquery
    SELECT s.*
    FROM kept_rows s
    WHERE EXISTS (
            SELECT 1
            FROM web_events ch
            WHERE ch.session_id = s.id
              AND ch.event_time <= s.started_at
        )
)
SELECT
    s.id,
    s.customer_id,
    s.device,
    s.started_at,
    s.country,
    s.is_new_session,
    s.island_id,
    CASE WHEN device RLIKE '^(desktop|tablet)$' THEN 'OTHER_ZONE' WHEN device RLIKE '^(mobile)$' THEN 'CORE' ELSE 'GROUP_A' END AS device_zone,
    (SELECT AVG(o2.amount) FROM orders o2 WHERE o2.customer_id = s.customer_id) AS avg_order_amount
FROM kept s;

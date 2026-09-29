WITH cleaned AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        customer_id,
        device,
        browser,
        started_at,
        country,
        CAST(started_at AS DATE) started_at_day,
        CASE WHEN browser IN ('Chrome', 'Firefox') THEN 'Group A' ELSE 'Other' END browser_group
    FROM sessions
    WHERE country IS NOT NULL
),
ranked AS (
    -- Keep one row per key (latest first)
    SELECT
        id,
        device,
        started_at
    FROM cleaned
    QUALIFY ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY started_at_day DESC, id) = 1
),
scored AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        ROW_NUMBER() OVER w_part row_seq,
        ROW_NUMBER() OVER w_part event_seq,
        ROW_NUMBER() OVER (ORDER BY started_at, id) row_seq_2,
        ROW_NUMBER() OVER w_part event_seq_2
    FROM ranked
    WHERE device IN ('desktop', 'tablet')
    WINDOW w_part AS (PARTITION BY device ORDER BY started_at, id)
),
gaps AS (
    -- Flag rows that start a new session (gap > interval)
    SELECT *, CASE WHEN started_at - LAG(started_at) OVER (PARTITION BY device ORDER BY started_at, id) > INTERVAL 7 DAYS THEN 1 ELSE 0 END AS session_start_flag
    FROM scored
),
islands AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(session_start_flag) OVER (PARTITION BY device ORDER BY started_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS burst_id
    FROM gaps
),
kept AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM islands s
    WHERE NOT EXISTS (
            SELECT 1
            FROM web_events c
            WHERE c.session_id = s.id
              AND c.event_time < s.started_at
        )
),
remaining_keys AS (
    -- Set difference on keys
    SELECT device, id
    FROM kept
    EXCEPT
    SELECT device, id
    FROM sessions
    WHERE browser IN ('Safari', 'Chrome')
),
kept_rows AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM kept s
    JOIN remaining_keys k ON s.device = k.device AND s.id = k.id
)
SELECT
    id,
    device,
    started_at,
    row_seq,
    row_seq_2,
    event_seq_2,
    session_start_flag
FROM kept_rows
ORDER BY started_at NULLS FIRST;

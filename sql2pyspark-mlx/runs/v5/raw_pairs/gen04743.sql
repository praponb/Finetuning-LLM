WITH flagged AS (
    -- Flag rows that start a new session (gap > interval)
    SELECT *, CASE WHEN started_at - LAG(started_at) OVER (PARTITION BY customer_id ORDER BY started_at, id) > INTERVAL 14 DAYS THEN 1 ELSE 0 END AS session_start_flag
    FROM sessions
),
sessionized AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(session_start_flag) OVER (PARTITION BY customer_id ORDER BY started_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_no
    FROM flagged
),
latest AS (
    -- QUALIFY-style window filter
    SELECT
        id,
        browser,
        country,
        session_start_flag
    FROM sessionized
    QUALIFY ROW_NUMBER() OVER (PARTITION BY device, customer_id ORDER BY started_at DESC, id) = 1
),
with_latest AS (
    -- Latest matching web_event per row (LATERAL ... LIMIT 1)
    SELECT
        s.id,
        s.country,
        s.session_start_flag,
        x.web_event_duration_sec,
        x.web_event_event_time
    FROM latest s
    LEFT JOIN LATERAL (
            SELECT c.duration_sec AS web_event_duration_sec, c.event_time AS web_event_event_time
            FROM web_events c
            WHERE c.session_id = s.id
            ORDER BY c.duration_sec DESC, c.id DESC
            LIMIT 1
        ) x ON TRUE
),
ranked AS (
    -- Deduplicate with ROW_NUMBER
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY country ORDER BY web_event_event_time DESC, id) AS dedup_rank
        FROM with_latest
    ) t
    WHERE dedup_rank = 1
),
with_last AS (
    -- Latest matching web_event per row (LATERAL ... LIMIT 1)
    SELECT
        s.id,
        s.session_start_flag,
        s.dedup_rank,
        top1.web_event_event_time_2,
        top1.event_type
    FROM ranked s
    LEFT JOIN LATERAL (
            SELECT c.event_time AS web_event_event_time_2, c.event_type
            FROM web_events c
            WHERE c.session_id = s.id
            ORDER BY c.duration_sec DESC, c.id DESC
            LIMIT 1
        ) top1 ON TRUE
),
kept_keys AS (
    -- Set intersection on keys
    SELECT id
    FROM with_last
    INTERSECT ALL
    SELECT id
    FROM sessions
    WHERE started_at < '2024-09-01'
),
reconciled AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM with_last s
    INNER JOIN kept_keys k ON s.id = k.id
),
kept AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM reconciled s
    WHERE NOT EXISTS (
            SELECT 1
            FROM web_events c
            WHERE c.session_id = s.id
              AND c.event_time <= s.web_event_event_time_2
        )
)
SELECT
    id,
    session_start_flag,
    dedup_rank,
    event_type,
    CASE WHEN event_type RLIKE '^(search|page_view)$' THEN 'GROUP_B' WHEN event_type RLIKE '^(click|add_to_cart)$' THEN 'EXTENDED' ELSE 'CORE' END AS event_type_zone
FROM kept
ORDER BY id;

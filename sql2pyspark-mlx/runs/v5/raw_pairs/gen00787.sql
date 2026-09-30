WITH gaps AS (
    -- Flag rows that start a new session (gap > interval)
    SELECT *, CASE WHEN LAG(started_at) OVER (PARTITION BY device ORDER BY started_at, id) IS NULL
                 OR started_at > LAG(started_at) OVER (PARTITION BY device ORDER BY started_at, id) + INTERVAL 14 DAYS THEN 1 ELSE 0 END AS new_group_flag
    FROM sessions
),
sessionized AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(new_group_flag) OVER (PARTITION BY device ORDER BY started_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_id
    FROM gaps
),
metrics AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        customer_id,
        country,
        session_id,
        MAX(new_group_flag) OVER (PARTITION BY device, customer_id ORDER BY session_id, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) AS last4_max_new_group_flag,
        AVG(new_group_flag) OVER (PARTITION BY device, customer_id ORDER BY session_id, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) AS last4_avg_new_group_flag,
        COUNT(new_group_flag) OVER (PARTITION BY device, customer_id ORDER BY session_id, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) AS last4_count_new_group_flag
    FROM sessionized
),
with_dims AS (
    -- Attach customers attributes
    SELECT
        e.id,
        e.customer_id,
        e.session_id,
        e.last4_max_new_group_flag,
        e.last4_avg_new_group_flag,
        e.last4_count_new_group_flag,
        d.name
    FROM metrics e
    LEFT JOIN customers d
        ON e.customer_id <=> d.id
),
with_latest AS (
    -- Latest matching web_event per row (LATERAL ... LIMIT 1)
    SELECT
        s.id,
        s.session_id,
        s.last4_max_new_group_flag,
        s.last4_count_new_group_flag,
        s.name,
        x.web_event_page
    FROM with_dims s
    LEFT JOIN LATERAL (
            SELECT c.page AS web_event_page
            FROM web_events c
            WHERE c.session_id = s.id
            ORDER BY c.duration_sec DESC, c.id DESC
            LIMIT 1
        ) x ON TRUE
)
SELECT
    id,
    session_id,
    last4_max_new_group_flag,
    last4_count_new_group_flag
FROM with_latest

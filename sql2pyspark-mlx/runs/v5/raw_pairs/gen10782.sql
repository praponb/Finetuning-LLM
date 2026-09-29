WITH latest AS (
    -- QUALIFY-style window filter
    SELECT
        id,
        session_id,
        event_type,
        page,
        event_time,
        duration_sec
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY session_id ORDER BY event_time NULLS FIRST, id) AS rank_in_group
        FROM web_events
    ) x
    WHERE rank_in_group = 1
),
enriched AS (
    -- Attach sessions attributes
    SELECT
        a.*,
        d.device,
        d.country
    FROM latest a
    LEFT JOIN sessions d
        ON a.session_id = d.id
    WHERE d.customer_id IN (15, 40)
),
ranked AS (
    -- Keep one row per key (latest first)
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY device ORDER BY event_time DESC NULLS LAST, id) AS dedup_rank
        FROM enriched
    ) x
    WHERE dedup_rank = 1
),
gaps AS (
    -- Flag rows that start a new session (gap > interval)
    SELECT *, CASE WHEN event_time - LAG(event_time) OVER (PARTITION BY page ORDER BY event_time, id) > INTERVAL 60 DAYS THEN 1 ELSE 0 END AS session_start_flag
    FROM ranked
),
sessionized AS (
    -- Running sum of the flag = session number
    SELECT
        id,
        session_id,
        event_type,
        page,
        event_time,
        duration_sec,
        device,
        country,
        SUM(session_start_flag) OVER (PARTITION BY page ORDER BY event_time, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_no
    FROM gaps
),
joined AS (
    -- Attach sessions attributes
    SELECT
        a.*,
        d.country AS session_country,
        d.browser
    FROM sessionized a
    INNER JOIN sessions d
        ON a.session_id = d.id
)
SELECT
    s.id,
    s.session_id,
    s.event_type,
    s.event_time,
    s.duration_sec,
    s.device,
    s.country,
    s.session_no,
    s.session_country,
    (SELECT MIN(t2.duration_sec) FROM web_events t2 WHERE t2.session_id = s.session_id) AS min_web_event_duration_sec
FROM joined s

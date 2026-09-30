WITH with_gaps AS (
    -- Flag rows that start a new session (gap > interval)
    SELECT *, CASE WHEN LAG(event_time) OVER (PARTITION BY page ORDER BY event_time, id) IS NULL
                 OR event_time > LAG(event_time) OVER (PARTITION BY page ORDER BY event_time, id) + INTERVAL 90 DAYS THEN 1 ELSE 0 END AS is_new_session
    FROM web_events
),
sessionized AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(is_new_session) OVER (PARTITION BY page ORDER BY event_time, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS burst_id
    FROM with_gaps
),
latest AS (
    -- Keep one row per key (latest first)
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY page ORDER BY is_new_session DESC NULLS LAST, id) AS rank_in_group
        FROM sessionized
    ) ranked
    WHERE rank_in_group = 1
),
gaps AS (
    -- Flag rows that start a new session (gap > interval)
    SELECT *, CASE WHEN event_time - LAG(event_time) OVER (PARTITION BY page ORDER BY event_time, id) > INTERVAL '14' DAY THEN 1 ELSE 0 END AS is_new_session_2
    FROM latest
),
sessions_web_events AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(is_new_session_2) OVER (PARTITION BY page ORDER BY event_time, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_no
    FROM gaps
),
joined AS (
    -- Attach sessions attributes
    SELECT
        a.*,
        p.browser,
        p.device,
        p.country
    FROM sessions_web_events a
    LEFT JOIN sessions p
        ON a.session_id = p.id
),
enriched AS (
    -- Ranking and gap features
    SELECT
        id,
        event_type,
        event_time,
        duration_sec,
        burst_id,
        is_new_session_2,
        device,
        country,
        SUM(burst_id) OVER w_recent AS last4_sum_burst_id,
        MAX(burst_id) OVER w_recent AS last4_max_burst_id,
        AVG(burst_id) OVER w_recent AS last4_avg_burst_id,
        COUNT(burst_id) OVER w_recent AS last4_count_burst_id,
        LEAD(session_no, 1) OVER (PARTITION BY browser, page ORDER BY rank_in_group, id) AS next_session_no
    FROM joined
    WINDOW w_recent AS (PARTITION BY session_id, browser ORDER BY duration_sec, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW)
)
SELECT
    id,
    event_type,
    burst_id,
    is_new_session_2,
    device,
    country,
    last4_max_burst_id,
    last4_avg_burst_id,
    last4_count_burst_id
FROM enriched
ORDER BY last4_count_burst_id DESC;

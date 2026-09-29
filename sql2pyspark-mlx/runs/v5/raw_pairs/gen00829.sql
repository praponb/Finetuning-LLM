WITH cleaned AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        customer_id,
        browser,
        started_at,
        country,
        CASE WHEN country = 'US' THEN 1 ELSE 0 END AS is_us,
        COALESCE(country, 'UNKNOWN') AS country_clean
    FROM sessions
    WHERE customer_id IS NULL
),
with_latest AS (
    -- Latest matching web_event per row (LATERAL ... LIMIT 1)
    SELECT
        s.*,
        top1.web_event_event_time,
        top1.event_type
    FROM cleaned s
    LEFT JOIN LATERAL (
            SELECT c.event_time AS web_event_event_time, c.event_type
            FROM web_events c
            WHERE c.session_id = s.id
            ORDER BY c.event_time DESC, c.id DESC
            LIMIT 1
        ) top1 ON TRUE
),
gaps AS (
    -- Flag rows that start a new session (gap > interval)
    SELECT *, CASE WHEN LAG(web_event_event_time) OVER (PARTITION BY browser ORDER BY web_event_event_time, id) IS NULL
                 OR web_event_event_time > LAG(web_event_event_time) OVER (PARTITION BY browser ORDER BY web_event_event_time, id) + INTERVAL 14 DAYS THEN 1 ELSE 0 END AS session_start_flag
    FROM with_latest
),
sessions_sessions AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(session_start_flag) OVER (PARTITION BY browser ORDER BY web_event_event_time, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_id
    FROM gaps
),
enriched AS (
    -- Ranking and gap features
    SELECT
        id,
        browser,
        started_at,
        country,
        web_event_event_time,
        event_type,
        session_start_flag,
        session_id,
        NTILE(5) OVER (PARTITION BY browser, country ORDER BY session_id DESC, id) AS session_id_bucket,
        SUM(session_start_flag) OVER (PARTITION BY customer_id ORDER BY session_start_flag, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_session_start_flag
    FROM sessions_sessions
),
scored AS (
    -- Running and moving aggregates
    SELECT
        *,
        LEAD(session_start_flag) OVER w_hist AS next_session_start_flag,
        LEAD(running_session_start_flag) OVER w_hist AS next_running_session_start_flag,
        ROW_NUMBER() OVER w_hist AS event_seq
    FROM enriched
    WHERE running_session_start_flag BETWEEN 2 AND 4
    WINDOW w_hist AS (PARTITION BY browser ORDER BY web_event_event_time, id)
)
SELECT
    id,
    browser,
    started_at,
    country,
    event_type,
    session_start_flag,
    session_id,
    session_id_bucket,
    running_session_start_flag
FROM scored
ORDER BY session_id DESC NULLS LAST;

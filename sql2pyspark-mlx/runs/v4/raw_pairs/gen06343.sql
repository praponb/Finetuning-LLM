WITH scored AS (
    -- Running and moving aggregates
    SELECT
        *,
        ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY started_at, id) AS seq_no,
        LAST_VALUE(browser) IGNORE NULLS OVER (PARTITION BY device ORDER BY started_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_browser,
        ROW_NUMBER() OVER (PARTITION BY device ORDER BY started_at DESC, id) AS event_seq
    FROM sessions
),
with_gaps AS (
    -- Flag rows that start a new session (gap > interval)
    SELECT *, CASE WHEN started_at - LAG(started_at) OVER (PARTITION BY customer_id ORDER BY started_at, id) > INTERVAL '7' DAY THEN 1 ELSE 0 END AS session_start_flag
    FROM scored
),
sessionized AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(session_start_flag) OVER (PARTITION BY customer_id ORDER BY started_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_id
    FROM with_gaps
),
enriched AS (
    -- Running and moving aggregates
    SELECT
        *,
        AVG(session_id) OVER w_recent AS session_id_avg_6,
        SUM(event_seq) OVER w_hist AS running_event_seq,
        FIRST_VALUE(event_seq) IGNORE NULLS OVER (PARTITION BY country ORDER BY started_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_event_seq
    FROM sessionized
    WINDOW w_recent AS (ORDER BY started_at, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW), w_hist AS (PARTITION BY browser ORDER BY event_seq, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
),
cleaned AS (
    -- Keep recent, valid rows
    SELECT
        *,
        running_event_seq * session_start_flag AS running_event_seq_times_session_start_flag,
        INITCAP(TRIM(country)) AS country_title,
        COALESCE(browser, 'DIRECT') AS browser_clean
    FROM enriched
    WHERE event_seq < 4
)
SELECT
    s.id,
    s.customer_id,
    s.device,
    s.started_at,
    s.seq_no,
    s.event_seq,
    s.session_start_flag,
    s.session_id,
    s.running_event_seq,
    CASE WHEN device RLIKE '^(tablet|desktop)$' THEN 'OTHER_ZONE' ELSE 'GROUP_A' END AS device_zone,
    (SELECT SUM(x.balance) FROM accounts x WHERE x.customer_id = s.customer_id) AS sum_account_balance
FROM cleaned s
ORDER BY seq_no

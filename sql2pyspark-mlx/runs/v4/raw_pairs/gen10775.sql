WITH scored AS (
    -- Running and moving aggregates
    SELECT
        *,
        ROW_NUMBER() OVER (PARTITION BY device ORDER BY started_at, id) AS seq_no,
        FIRST_VALUE(country) IGNORE NULLS OVER (PARTITION BY browser ORDER BY started_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_country,
        FIRST_VALUE(browser) IGNORE NULLS OVER (PARTITION BY customer_id ORDER BY started_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_browser,
        ROW_NUMBER() OVER (PARTITION BY browser ORDER BY started_at DESC, id) AS seq_no_2
    FROM sessions
),
cleaned AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        *,
        DATE_FORMAT(started_at, 'yyyy-MM') AS started_at_label,
        CAST(seq_no AS STRING) AS seq_no_num
    FROM scored
    WHERE started_at >= DATE '2023-07-14'
),
with_last AS (
    -- Latest matching web_event per row (LATERAL ... LIMIT 1)
    SELECT s.*, top1.web_event_page
    FROM cleaned s
    JOIN LATERAL (
            SELECT c.page AS web_event_page
            FROM web_events c
            WHERE c.session_id = s.id
            ORDER BY c.duration_sec DESC, c.id DESC
            LIMIT 1
        ) top1 ON TRUE
),
filtered AS (
    -- Keep recent, valid rows
    SELECT
        id,
        customer_id,
        browser,
        started_at,
        country,
        seq_no,
        started_at_label,
        seq_no_num,
        web_event_page,
        LENGTH(web_event_page) AS web_event_page_length
    FROM with_last
    WHERE country IN ('TH', 'JP')
),
flagged AS (
    -- Flag rows that start a new session (gap > interval)
    SELECT *, CASE WHEN started_at - LAG(started_at) OVER (PARTITION BY customer_id ORDER BY started_at, id) > INTERVAL '30' DAY THEN 1 ELSE 0 END AS is_new_session
    FROM filtered
),
islands AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(is_new_session) OVER (PARTITION BY customer_id ORDER BY started_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_no
    FROM flagged
)
-- Ranking and gap features
SELECT
    *,
    MAX(seq_no) OVER w_part AS seq_no_max_7d,
    AVG(session_no) OVER w_grp AS session_no_avg_6,
    SUM(is_new_session) OVER (PARTITION BY browser, country) AS browser_sum_is_new_session,
    NTILE(5) OVER (ORDER BY is_new_session, id) AS is_new_session_bucket
FROM islands
WINDOW w_part AS (ORDER BY CAST(started_at AS BIGINT) RANGE BETWEEN 604800 PRECEDING AND CURRENT ROW), w_grp AS (PARTITION BY country, browser ORDER BY web_event_page_length, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW)

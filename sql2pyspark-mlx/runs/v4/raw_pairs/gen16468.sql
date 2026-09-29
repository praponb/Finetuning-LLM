WITH filtered AS (
    -- Keep recent, valid rows
    SELECT
        id,
        browser,
        started_at,
        country,
        LOWER(browser) AS browser_lower
    FROM sessions
    WHERE customer_id IS NOT NULL
),
latest AS (
    -- QUALIFY-style window filter
    SELECT *
    FROM filtered
    QUALIFY ROW_NUMBER() OVER (PARTITION BY country, browser ORDER BY started_at ASC NULLS LAST, id ASC) = 1
),
metrics AS (
    -- Ranking and gap features
    SELECT
        id,
        browser,
        started_at,
        country,
        browser_lower,
        ROW_NUMBER() OVER (PARTITION BY browser ORDER BY started_at ASC, id ASC) AS event_seq,
        LAST_VALUE(country, TRUE) OVER (PARTITION BY browser, country ORDER BY started_at ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_country,
        LAST_VALUE(country, TRUE) OVER (PARTITION BY browser, country ORDER BY started_at ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_country_2,
        ROW_NUMBER() OVER (PARTITION BY country ORDER BY started_at ASC, id ASC) AS row_seq,
        LAST_VALUE(country, TRUE) OVER (PARTITION BY browser ORDER BY started_at ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_country_3
    FROM latest
),
with_latest AS (
    -- Latest matching web_event per row (LATERAL ... LIMIT 1)
    SELECT
        s.*,
        latest.web_event_page,
        latest.duration_sec
    FROM metrics s
    LEFT JOIN LATERAL (
            SELECT c.page AS web_event_page, c.duration_sec
            FROM web_events c
            WHERE c.session_id = s.id
              AND c.event_time <= s.started_at
            ORDER BY c.event_time DESC, c.id DESC
            LIMIT 1
        ) latest ON TRUE
),
scored AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        LEAD(row_seq, 1) OVER (PARTITION BY country ORDER BY started_at ASC, id ASC) AS next_row_seq,
        ROW_NUMBER() OVER (PARTITION BY country ORDER BY duration_sec ASC, id ASC) AS seq_no
    FROM with_latest
),
flagged AS (
    -- Flag rows that start a new session (gap > interval)
    SELECT *, CASE WHEN started_at - LAG(started_at) OVER (PARTITION BY country ORDER BY started_at ASC, id ASC) > INTERVAL 60 DAYS THEN 1 ELSE 0 END AS is_new_burst
    FROM scored
),
sessions_sessions AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(is_new_burst) OVER (PARTITION BY country ORDER BY started_at ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS burst_id
    FROM flagged
),
with_prev AS (
    -- Previous event time per key
    SELECT *, LAG(started_at) OVER (PARTITION BY country ORDER BY started_at ASC, id ASC) AS prev_started_at
    FROM sessions_sessions
),
gaps AS (
    -- Flag rows that start a new session
    SELECT *, CASE WHEN prev_started_at IS NULL OR started_at > prev_started_at + INTERVAL 30 DAYS THEN 1 ELSE 0 END AS new_group_flag
    FROM with_prev
),
sessionized AS (
    -- Running sum of the flag = session number
    SELECT *, SUM(new_group_flag) OVER (PARTITION BY country ORDER BY started_at ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_no
    FROM gaps
)
-- Aggregate with conditional metrics
SELECT
    CASE WHEN event_seq >= 8 THEN 'PREMIUM' ELSE 'STANDARD' END AS event_seq_tier,
    SUM(CASE WHEN browser = 'Chrome' THEN seq_no ELSE 0 END) AS chrome_seq_no,
    COUNT(DISTINCT browser) AS distinct_browsers,
    SIZE(COLLECT_SET(web_event_page)) AS num_web_event_pages,
    COUNT_IF(country = 'UK') AS uk_events,
    COUNT(CASE WHEN country = 'DE' THEN 1 END) AS de_count
FROM sessionized
GROUP BY 1

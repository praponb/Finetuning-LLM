WITH enriched AS (
    SELECT
        id,
        device,
        browser,
        country,
        ROW_NUMBER() OVER (PARTITION BY device ORDER BY started_at, id) seq_no
    FROM sessions
),
with_windows AS (
    SELECT
        id,
        browser,
        seq_no,
        SUM(seq_no) OVER (PARTITION BY country ORDER BY seq_no, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) running_sum_seq_no,
        MIN(seq_no) OVER (PARTITION BY country ORDER BY seq_no, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) running_min_seq_no,
        AVG(seq_no) OVER (PARTITION BY country ORDER BY seq_no, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) running_avg_seq_no,
        MAX(seq_no) OVER (PARTITION BY country ORDER BY seq_no, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) running_max_seq_no,
        CUME_DIST() OVER (PARTITION BY device ORDER BY seq_no) seq_no_cume_dist
    FROM enriched
),
eligible AS (
    SELECT s.*
    FROM with_windows s
    WHERE EXISTS (
            SELECT 1
            FROM web_events c
            WHERE c.session_id = s.id
              AND c.event_type != 'page_view'
        )
),
with_latest AS (
    SELECT
        s.id,
        s.browser,
        s.seq_no,
        s.running_sum_seq_no,
        s.running_avg_seq_no,
        latest.page
    FROM eligible s
    JOIN LATERAL (
            SELECT c.page
            FROM web_events c
            WHERE c.session_id = s.id
            ORDER BY c.event_time DESC, c.id DESC
            LIMIT 1
        ) latest ON TRUE
),
eligible_sessions AS (
    SELECT s.*
    FROM with_latest s
    WHERE EXISTS (
            SELECT 1
            FROM web_events x
            WHERE x.session_id = s.id
              AND x.page = '/product'
        )
),
with_last AS (
    SELECT
        s.id,
        s.browser,
        s.running_sum_seq_no,
        s.page,
        latest.web_event_event_time
    FROM eligible_sessions s
    LEFT JOIN LATERAL (
            SELECT c.event_time AS web_event_event_time
            FROM web_events c
            WHERE c.session_id = s.id
            ORDER BY c.event_time DESC, c.id DESC
            LIMIT 1
        ) latest ON TRUE
),
latest AS (
    SELECT
        id,
        browser,
        web_event_event_time
    FROM (
        SELECT *, RANK() OVER (PARTITION BY browser, page ORDER BY running_sum_seq_no NULLS LAST) AS dedup_rank
        FROM with_last
    ) x
    WHERE dedup_rank = 1
)
SELECT
    browser,
    DATE_TRUNC('YEAR', web_event_event_time) AS year_start,
    MIN(web_event_event_time) AS first_web_event_event_time,
    (UNIX_TIMESTAMP(MAX(web_event_event_time)) - UNIX_TIMESTAMP(MIN(web_event_event_time))) / 60 AS duration_min,
    COUNT(*) FILTER (WHERE browser = 'Firefox') AS firefox_rows
FROM latest
GROUP BY browser, DATE_TRUNC('YEAR', web_event_event_time)
ORDER BY browser DESC

WITH with_windows AS (
    -- Running and moving aggregates
    SELECT
        *,
        FIRST_VALUE(browser, TRUE) OVER (PARTITION BY customer_id, country ORDER BY started_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_browser,
        ROW_NUMBER() OVER (PARTITION BY browser, device ORDER BY started_at, id) AS event_seq,
        FIRST_VALUE(browser) IGNORE NULLS OVER w_ordered AS first_known_browser_2
    FROM sessions
    WINDOW w_ordered AS (ORDER BY started_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
),
kept AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM with_windows s
    WHERE NOT EXISTS (
            SELECT 1
            FROM web_events ch
            WHERE ch.session_id = s.id
              AND ch.page = '/home'
        )
),
with_windows_sessions AS (
    -- Ranking and gap features
    SELECT
        *,
        AVG(event_seq) OVER (ORDER BY started_at, id ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING) AS event_seq_avg_3_prior,
        ROUND(event_seq / SUM(event_seq) OVER (PARTITION BY country), 4) AS event_seq_share
    FROM kept
)
-- Multi-level aggregation
SELECT
    DATE_TRUNC('QUARTER', started_at) AS quarter_start,
    SORT_ARRAY(COLLECT_SET(browser)) AS browser_list,
    SUM(CASE WHEN browser = 'Firefox' THEN event_seq_avg_3_prior ELSE 0 END) AS firefox_event_seq_avg_3_prior,
    MIN(started_at) AS first_seen,
    APPROX_PERCENTILE(event_seq, 0.95) AS event_seq_p95,
    SUM(CASE WHEN device = 'mobile' THEN event_seq_avg_3_prior ELSE 0 END) AS mobile_event_seq_avg_3_prior,
    MAX(started_at) AS ended_at
FROM with_windows_sessions
GROUP BY DATE_TRUNC('QUARTER', started_at)
ORDER BY event_seq_p95 DESC NULLS LAST;

WITH filtered AS (
    SELECT
        id,
        device,
        browser,
        started_at,
        country,
        CONCAT_WS('-', device, country) device_country_key,
        COALESCE(browser, 'OTHER') browser_filled
    FROM sessions
    WHERE country <> 'JP'
),
with_prev AS (
    SELECT *, LAG(started_at) OVER (PARTITION BY device ORDER BY started_at, id) AS prev_started_at
    FROM filtered
),
flagged AS (
    SELECT *, CASE WHEN prev_started_at IS NULL OR started_at > prev_started_at + INTERVAL 14 DAYS THEN 1 ELSE 0 END AS session_start_flag
    FROM with_prev
),
islands AS (
    SELECT *, SUM(session_start_flag) OVER (PARTITION BY device ORDER BY started_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_id
    FROM flagged
),
metrics AS (
    SELECT
        id,
        browser,
        device_country_key,
        session_start_flag,
        SUM(session_id) OVER (PARTITION BY country ORDER BY CAST(prev_started_at AS BIGINT) RANGE BETWEEN 7776000 PRECEDING AND CURRENT ROW) session_id_sum_90d,
        COUNT(session_id) OVER (ORDER BY CAST(prev_started_at AS BIGINT) RANGE BETWEEN 7776000 PRECEDING AND CURRENT ROW) session_id_count_90d,
        AVG(session_id) OVER w_running session_id_avg_6_prior,
        ROW_NUMBER() OVER w_grp event_seq,
        AVG(session_id) OVER (ORDER BY session_start_flag, id ROWS BETWEEN 5 PRECEDING AND CURRENT ROW) session_id_avg_5
    FROM islands
    WHERE prev_started_at < '2023-03-09'
    WINDOW w_running AS (PARTITION BY country ORDER BY prev_started_at, id ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING), w_grp AS (PARTITION BY browser ORDER BY started_at, id)
),
with_latest AS (
    SELECT
        s.id,
        s.browser,
        s.session_start_flag,
        s.session_id_sum_90d,
        s.session_id_count_90d,
        s.session_id_avg_6_prior,
        s.event_seq,
        s.session_id_avg_5,
        latest.web_event_duration_sec
    FROM metrics s
    LEFT JOIN LATERAL (
            SELECT c.duration_sec AS web_event_duration_sec
            FROM web_events c
            WHERE c.session_id = s.id
            ORDER BY c.event_time DESC, c.id DESC
            LIMIT 1
        ) latest ON TRUE
),
with_latest_sessions AS (
    SELECT
        s.id,
        s.browser,
        s.session_id_avg_6_prior,
        s.web_event_duration_sec,
        latest.web_event_event_type
    FROM with_latest s
    JOIN LATERAL (
            SELECT c.event_type AS web_event_event_type
            FROM web_events c
            WHERE c.session_id = s.id
            ORDER BY c.event_time DESC, c.id DESC
            LIMIT 1
        ) latest ON TRUE
)
SELECT
    CASE WHEN web_event_duration_sec >= 476 THEN 'HIGH' ELSE 'LOW' END AS web_event_duration_sec_tier,
    browser,
    SUM(CASE WHEN browser = 'Firefox' THEN session_id_avg_6_prior ELSE 0 END) AS firefox_session_id_avg_6_prior,
    PERCENTILE_APPROX(web_event_duration_sec, ARRAY(0.25, 0.5, 0.75)) AS web_event_duration_sec_quartiles,
    SIZE(COLLECT_SET(browser)) AS num_browsers,
    COUNT(CASE WHEN browser = 'Safari' THEN 1 END) AS safari_count
FROM with_latest_sessions
GROUP BY CASE WHEN web_event_duration_sec >= 476 THEN 'HIGH' ELSE 'LOW' END, browser
ORDER BY browser DESC

WITH enriched AS (
    -- Attach sessions attributes
    SELECT
        a.id,
        a.session_id,
        a.event_time,
        a.duration_sec,
        s.browser
    FROM web_events a
    LEFT JOIN sessions s
        ON a.session_id = s.id
),
with_dims AS (
    -- Attach sessions attributes
    SELECT
        s.id,
        s.session_id,
        s.duration_sec,
        s.browser,
        d.country,
        d.browser AS session_browser,
        d.device
    FROM enriched s
    LEFT JOIN sessions d
        ON s.session_id = d.id
),
enriched_web_events AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        SUM(duration_sec) OVER w_recent AS running_duration_sec,
        SUM(duration_sec) OVER (PARTITION BY browser) AS browser_sum_duration_sec,
        AVG(duration_sec) OVER w_running AS duration_sec_avg_3_prior
    FROM with_dims
    WHERE session_browser IN ('Chrome', 'Firefox')
    WINDOW w_recent AS (PARTITION BY session_id ORDER BY duration_sec, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW), w_running AS (ORDER BY duration_sec, id ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING)
),
latest AS (
    -- Keep one row per key (latest first)
    SELECT *, ROW_NUMBER() OVER (PARTITION BY country, browser ORDER BY duration_sec_avg_3_prior DESC, id) AS dedup_rank
    FROM enriched_web_events
    QUALIFY dedup_rank = 1
),
joined AS (
    -- Attach sessions attributes
    SELECT
        src.id,
        src.session_id,
        src.duration_sec,
        src.device,
        src.running_duration_sec,
        src.duration_sec_avg_3_prior,
        src.dedup_rank,
        s.country AS session_country
    FROM latest src
    JOIN sessions s
        ON src.session_id = s.id
)
-- Multi-level aggregation
SELECT
    CASE WHEN duration_sec >= 422 THEN 'PREMIUM' ELSE 'STANDARD' END AS duration_sec_tier,
    PERCENTILE_APPROX(duration_sec_avg_3_prior, 0.9) AS duration_sec_avg_3_prior_p90,
    COUNT(CASE WHEN device = 'tablet' THEN 1 END) AS tablet_count,
    COUNT(CASE WHEN session_country = 'JP' THEN 1 END) AS jp_count,
    ROUND(AVG(running_duration_sec), 2) AS avg_running_duration_sec,
    SIZE(COLLECT_SET(session_country)) AS num_session_countries
FROM joined
GROUP BY 1;

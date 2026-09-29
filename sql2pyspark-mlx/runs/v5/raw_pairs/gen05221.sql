WITH cleaned AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        customer_id,
        device,
        browser,
        started_at,
        CASE WHEN country = 'DE' THEN 1 ELSE 0 END AS is_de,
        COALESCE(country, 'OTHER') AS resolved_country
    FROM sessions
    WHERE country IS DISTINCT FROM 'JP'
),
joined AS (
    -- Attach customers attributes
    SELECT
        s.id,
        s.customer_id,
        s.device,
        s.started_at,
        s.resolved_country,
        d.email,
        d.city,
        d.name
    FROM cleaned s
    LEFT JOIN customers d
        ON s.customer_id <=> d.id
),
flagged AS (
    -- Flag rows that start a new session (gap > interval)
    SELECT *, CASE WHEN started_at - LAG(started_at) OVER (PARTITION BY customer_id ORDER BY started_at, id) > INTERVAL 90 DAYS THEN 1 ELSE 0 END AS is_new_session
    FROM joined
),
sessionized AS (
    -- Running sum of the flag = session number
    SELECT
        id,
        customer_id,
        device,
        started_at,
        name,
        SUM(is_new_session) OVER (PARTITION BY customer_id ORDER BY started_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_no
    FROM flagged
),
kept AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM sessionized s
    WHERE NOT EXISTS (
            SELECT 1
            FROM web_events ch
            WHERE ch.session_id = s.id
              AND ch.event_time >= s.started_at
        )
),
with_windows AS (
    -- Ranking and gap features
    SELECT
        id,
        device,
        name,
        SUM(session_no) OVER w_running AS cumulative_session_no,
        MIN(session_no) OVER (PARTITION BY customer_id) AS customer_id_min_session_no,
        PERCENT_RANK() OVER (PARTITION BY customer_id ORDER BY session_no) AS session_no_percent_rank
    FROM kept
    WINDOW w_running AS (PARTITION BY device ORDER BY started_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
),
with_windows_sessions AS (
    -- Ranking and gap features
    SELECT
        *,
        SUM(customer_id_min_session_no) OVER (PARTITION BY device ORDER BY customer_id_min_session_no, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_customer_id_min_session_no,
        PERCENT_RANK() OVER (ORDER BY cumulative_session_no NULLS FIRST) AS cumulative_session_no_percent_rank
    FROM with_windows
),
stats AS (
    -- Grouped metrics
    SELECT
        CASE WHEN cumulative_session_no_percent_rank >= 0.76 THEN 'PREMIUM' ELSE 'STANDARD' END AS cumulative_session_no_percent_rank_tier,
        CASE WHEN customer_id_min_session_no >= 6 THEN 'PREMIUM' ELSE 'STANDARD' END AS customer_id_min_session_no_tier,
        COUNT(*) FILTER (WHERE device = 'desktop') AS desktop_rows,
        COUNT(DISTINCT device) AS distinct_devices,
        SORT_ARRAY(COLLECT_SET(device)) AS device_list,
        PERCENTILE_APPROX(cumulative_session_no, 0.9) AS cumulative_session_no_p90,
        COUNT(*) AS num_rows
    FROM with_windows_sessions
    GROUP BY CASE WHEN cumulative_session_no_percent_rank >= 0.76 THEN 'PREMIUM' ELSE 'STANDARD' END, CASE WHEN customer_id_min_session_no >= 6 THEN 'PREMIUM' ELSE 'STANDARD' END
)
SELECT
    cumulative_session_no_percent_rank_tier,
    customer_id_min_session_no_tier,
    desktop_rows,
    device_list
FROM stats
ORDER BY desktop_rows NULLS LAST;

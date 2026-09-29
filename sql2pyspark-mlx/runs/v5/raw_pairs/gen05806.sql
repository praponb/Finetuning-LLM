WITH scored AS (
    -- Ranking and gap features
    SELECT
        *,
        LAG(duration_sec, 1) OVER (PARTITION BY event_type ORDER BY event_time, id) AS prev_duration_sec,
        LEAD(duration_sec, 1) OVER (ORDER BY duration_sec, id) AS next_duration_sec
    FROM web_events
),
with_dims AS (
    -- Attach sessions attributes
    SELECT
        s.id,
        s.session_id,
        s.event_time,
        s.prev_duration_sec,
        s.next_duration_sec,
        b.country,
        b.browser,
        b.device
    FROM scored s
    LEFT JOIN sessions b
        ON s.session_id = b.id
    WHERE b.device != 'desktop'
),
joined AS (
    -- Attach sessions attributes
    SELECT s.*, d.country AS session_country
    FROM with_dims s
    LEFT JOIN sessions d
        ON s.session_id = d.id
),
joined_web_events AS (
    -- Attach sessions attributes
    SELECT
        e.id,
        e.session_id,
        e.prev_duration_sec,
        e.next_duration_sec,
        e.country,
        e.browser,
        d.country AS session_country_2,
        d.browser AS session_browser,
        d.device AS session_device
    FROM joined e
    LEFT JOIN sessions d
        ON e.session_id = d.id
),
enriched AS (
    -- Ranking and gap features
    SELECT
        id,
        prev_duration_sec,
        next_duration_sec,
        browser,
        session_browser,
        MAX(prev_duration_sec) OVER w_running AS last4_max_prev_duration_sec,
        MIN(prev_duration_sec) OVER w_running AS last4_min_prev_duration_sec,
        AVG(prev_duration_sec) OVER w_running AS last4_avg_prev_duration_sec,
        SUM(prev_duration_sec) OVER w_running AS last4_sum_prev_duration_sec
    FROM joined_web_events
    WINDOW w_running AS (PARTITION BY session_browser ORDER BY next_duration_sec, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW)
),
remaining_keys AS (
    -- Set difference on keys
    SELECT id
    FROM enriched
    EXCEPT ALL
    SELECT id
    FROM web_events
    WHERE duration_sec BETWEEN 240 AND 340
),
verified AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM enriched s
    JOIN remaining_keys k ON s.id = k.id
)
-- Aggregate with conditional metrics
SELECT
    session_browser,
    APPROX_PERCENTILE(last4_avg_prev_duration_sec, ARRAY(0.25, 0.5, 0.75)) AS last4_avg_prev_duration_sec_quartiles,
    SUM(CASE WHEN browser = 'Chrome' THEN last4_avg_prev_duration_sec ELSE 0 END) AS chrome_last4_avg_prev_duration_sec,
    SUM(CASE WHEN browser = 'Firefox' THEN prev_duration_sec ELSE 0 END) AS firefox_prev_duration_sec
FROM verified
GROUP BY session_browser

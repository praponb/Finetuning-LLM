WITH with_windows AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        ROW_NUMBER() OVER w_running AS event_seq,
        ROW_NUMBER() OVER (PARTITION BY page ORDER BY event_time DESC, id) AS row_seq
    FROM web_events
    WINDOW w_running AS (ORDER BY duration_sec DESC, id)
)
-- Grouped metrics
SELECT
    CASE WHEN row_seq >= 7 THEN 'PREMIUM' ELSE 'STANDARD' END AS row_seq_tier,
    session_id,
    page,
    MAX(event_time) AS last_event_time,
    AVG(event_seq) FILTER (WHERE event_type = 'search') AS search_avg_event_seq,
    APPROX_PERCENTILE(event_seq, 0.5) AS event_seq_p50,
    BIT_OR(duration_sec) AS duration_sec_bit_or,
    ROUND(SUM(duration_sec) / NULLIF(COUNT(CASE WHEN page = '/cart' THEN 1 END), 0), 2) AS duration_sec_per_cart,
    COUNT(*) AS row_count
FROM with_windows
GROUP BY CASE WHEN row_seq >= 7 THEN 'PREMIUM' ELSE 'STANDARD' END, session_id, page;

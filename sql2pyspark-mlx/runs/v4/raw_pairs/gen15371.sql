WITH with_windows AS (
    SELECT
        *,
        ROW_NUMBER() OVER w_running AS event_seq,
        ROW_NUMBER() OVER (PARTITION BY device, customer_id ORDER BY started_at DESC, id) AS seq_no
    FROM sessions
    WINDOW w_running AS (PARTITION BY device ORDER BY started_at, id)
)
SELECT
    CASE WHEN event_seq >= 8 THEN 'HIGH' ELSE 'LOW' END AS event_seq_tier,
    COUNT_IF(device = 'desktop') AS desktop_events,
    AVG(event_seq) FILTER (WHERE browser = 'Firefox') AS firefox_avg_event_seq,
    COUNT(CASE WHEN country = 'JP' THEN 1 END) AS jp_count,
    SUM(CASE WHEN device = 'desktop' THEN event_seq ELSE 0 END) AS desktop_event_seq,
    ROUND(AVG(event_seq), 2) AS avg_event_seq
FROM with_windows
GROUP BY 1

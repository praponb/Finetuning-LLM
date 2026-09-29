WITH session_totals AS (
    SELECT session_id, SORT_ARRAY(COLLECT_SET(event_type)) event_type_list, ROUND(STDDEV(duration_sec), 2) stddev_duration_sec
    FROM web_events
    GROUP BY session_id
)
SELECT
    s.id,
    s.started_at,
    s.customer_id,
    st.event_type_list,
    st.stddev_duration_sec
FROM sessions s
LEFT JOIN session_totals st ON s.id = st.session_id
ORDER BY started_at DESC;

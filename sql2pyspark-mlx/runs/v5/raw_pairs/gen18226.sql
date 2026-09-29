WITH session_totals AS (
    SELECT session_id, MAX(duration_sec) - MIN(duration_sec) duration_sec_range, CONCAT_WS(', ', SORT_ARRAY(COLLECT_SET(page))) page_values
    FROM web_events
    GROUP BY session_id
)
SELECT
    s.id,
    s.customer_id,
    st.duration_sec_range,
    st.page_values
FROM sessions s
LEFT JOIN session_totals st ON s.id = st.session_id

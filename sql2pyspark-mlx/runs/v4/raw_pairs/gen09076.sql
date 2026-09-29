WITH SessionTotals AS (
    SELECT session_id, MAX(duration_sec) - MIN(duration_sec) AS duration_sec_range, SORT_ARRAY(COLLECT_SET(event_type)) AS event_type_list
    FROM web_events
    WHERE HOUR(event_time) BETWEEN 8 AND 12
    GROUP BY session_id
)
SELECT s.customer_id, S.duration_sec_range, S.event_type_list
FROM sessions s
INNER JOIN SessionTotals S ON s.id = S.session_id
ORDER BY customer_id, duration_sec_range DESC;

WITH web_events_by_session AS (
    SELECT session_id, ROUND((100 * SUM(CASE WHEN event_type = 'add_to_cart' THEN 1 ELSE 0 END)) / COUNT(*), 1) AS pct_add_to_cart
    FROM web_events
    WHERE page = '/checkout'
    GROUP BY session_id
)
SELECT s.customer_id, s.started_at, webs.pct_add_to_cart
FROM sessions s
JOIN web_events_by_session webs ON s.id = webs.session_id;

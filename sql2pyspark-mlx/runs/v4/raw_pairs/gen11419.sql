WITH filtered_web_events AS (
    SELECT *
    FROM web_events
    WHERE event_type IN ('add_to_cart', 'click', 'search') AND page IN ('/home', '/product') AND session_id IN (33, 39)
),
agg AS (
    SELECT event_type, ROUND((100 * SUM(CASE WHEN event_type = 'click' THEN 1 ELSE 0 END)) / COUNT(*), 1) AS pct_click, ROUND((100 * SUM(CASE WHEN event_type = 'page_view' THEN 1 ELSE 0 END)) / COUNT(*), 1) AS pct_page_view
    FROM filtered_web_events
    GROUP BY event_type
)
SELECT *
FROM agg;

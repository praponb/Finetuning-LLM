SELECT ROUND((100 * SUM(CASE WHEN page = '/product' THEN 1 ELSE 0 END)) / COUNT(*), 1) AS pct_product FROM web_events WHERE event_type = 'click' AND page = '/search' ORDER BY pct_product DESC;

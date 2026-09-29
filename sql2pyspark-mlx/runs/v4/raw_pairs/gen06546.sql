SELECT CASE WHEN event_type IN ('search', 'add_to_cart') THEN 'group_a' ELSE 'other' END AS event_type_group, SUM(duration_sec) AS total_duration_sec, DENSE_RANK() OVER (ORDER BY SUM(duration_sec) DESC) AS rnk
FROM web_events
GROUP BY CASE WHEN event_type IN ('search', 'add_to_cart') THEN 'group_a' ELSE 'other' END;

SELECT page, CASE WHEN event_type IN ('search', 'page_view') THEN 'tier_1' ELSE 'other' END AS event_type_group, SUM(CASE WHEN event_type = 'purchase' THEN 1 ELSE 0 END) AS purchase_count
FROM web_events
GROUP BY 1, 2
ORDER BY page, purchase_count DESC;

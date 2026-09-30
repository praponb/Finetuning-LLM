SELECT SORT_ARRAY(COLLECT_SET(status)) AS status_list, ROUND((100 * SUM(CASE WHEN channel = 'partner' THEN 1 ELSE 0 END)) / COUNT(*), 1) AS pct_partner
FROM orders

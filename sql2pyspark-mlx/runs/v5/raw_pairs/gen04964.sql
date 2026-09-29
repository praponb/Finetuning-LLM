SELECT DATE_TRUNC('MONTH', o.order_date) AS order_month, SORT_ARRAY(COLLECT_SET(p.status)) AS status_list, ROUND((100 * SUM(CASE WHEN p.method = 'card' THEN 1 ELSE 0 END)) / COUNT(*), 1) AS pct_card
FROM payments p
INNER JOIN orders o ON o.id = p.order_id
GROUP BY DATE_TRUNC('MONTH', o.order_date)
ORDER BY pct_card DESC

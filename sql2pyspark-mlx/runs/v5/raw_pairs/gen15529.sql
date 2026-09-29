SELECT CASE WHEN YEAR(o.order_date) < 2024 THEN 'before_2024' WHEN YEAR(o.order_date) = 2024 THEN '2024' ELSE 'after_2024' END AS order_cohort, SUM(CASE WHEN s.status = 'IN_TRANSIT' THEN 1 ELSE 0 END) AS in_transit_count, ROUND((100 * SUM(CASE WHEN s.status = 'DELIVERED' THEN 1 ELSE 0 END)) / COUNT(*), 1) AS pct_delivered
FROM shipments s
JOIN orders o ON s.order_id = o.id
WHERE s.delivered_date > '2025-09-07'
GROUP BY CASE WHEN YEAR(o.order_date) < 2024 THEN 'before_2024' WHEN YEAR(o.order_date) = 2024 THEN '2024' ELSE 'after_2024' END
ORDER BY order_cohort DESC;

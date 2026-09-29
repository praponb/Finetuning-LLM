SELECT DATE_FORMAT(p.launch_date, 'yyyy-MM') AS launch_month, AVG(ss.revenue) AS avg_revenue, COUNT(*) AS total_count
FROM store_sales ss
FULL OUTER JOIN products p ON ss.product_id = p.id
WHERE ss.product_id IN (1, 4)
GROUP BY DATE_FORMAT(p.launch_date, 'yyyy-MM')

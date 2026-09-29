SELECT QUARTER(p.launch_date) AS launch_quarter, SUM(ss.revenue) AS total_revenue
FROM store_sales ss
LEFT JOIN products p ON ss.product_id = p.id
INNER JOIN suppliers s ON p.supplier_id = s.id
GROUP BY QUARTER(p.launch_date)
ORDER BY total_revenue, launch_quarter
LIMIT 10

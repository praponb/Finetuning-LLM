SELECT p.category, ROUND(STDDEV(ss.revenue), 2) AS stddev_revenue, MAX(ss.revenue) - MIN(ss.revenue) AS revenue_range
FROM store_sales ss
LEFT JOIN stores s ON ss.store_id = s.id
LEFT JOIN products p ON ss.product_id = p.id
WHERE p.cost BETWEEN 310 AND 640
GROUP BY p.category
HAVING ROUND(STDDEV(ss.revenue), 2) >= 47
ORDER BY revenue_range ASC

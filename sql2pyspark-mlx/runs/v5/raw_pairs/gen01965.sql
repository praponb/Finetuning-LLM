SELECT s.city, ROUND(STDDEV(ss.revenue), 2) AS stddev_revenue
FROM store_sales ss
LEFT JOIN stores s ON s.id = ss.store_id
WHERE s.store_name LIKE 'A%'
GROUP BY s.city
ORDER BY city DESC

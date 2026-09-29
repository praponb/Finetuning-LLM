SELECT s.city, s.region, MAX(ss.sale_date) AS last_sale_date
FROM store_sales ss
JOIN stores s ON ss.store_id = s.id
GROUP BY s.city, s.region
ORDER BY last_sale_date ASC

SELECT MONTH(s.opened_date) AS opened_month, AVG(ss.revenue) AS avg_revenue, MIN(ss.sale_date) AS first_sale_date
FROM store_sales ss
LEFT JOIN products p ON p.id = ss.product_id
INNER JOIN stores s ON ss.store_id = s.id
WHERE ss.units < 16
GROUP BY MONTH(s.opened_date)

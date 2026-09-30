SELECT p.brand, p.category, MIN(ss.revenue) AS min_revenue
FROM store_sales ss
INNER JOIN products p ON ss.product_id = p.id
INNER JOIN suppliers s ON p.supplier_id = s.id
WHERE s.city = 'Berlin'
GROUP BY p.brand, p.category
HAVING MIN(ss.revenue) >= 1300

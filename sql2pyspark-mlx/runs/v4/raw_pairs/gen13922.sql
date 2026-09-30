SELECT p.category, MAX(ss.revenue) - MIN(ss.revenue) AS revenue_range, COUNT(*) AS total_count
FROM store_sales ss
LEFT OUTER JOIN products p ON p.id = ss.product_id
JOIN suppliers s ON p.supplier_id = s.id
WHERE p.product_name LIKE '%Running%'
GROUP BY p.category;

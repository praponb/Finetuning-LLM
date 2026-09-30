SELECT s.country, ROUND(STDDEV(ss.revenue), 2) stddev_revenue, MAX(ss.revenue) max_revenue
FROM store_sales ss
INNER JOIN products p ON ss.product_id = p.id
LEFT JOIN suppliers s ON s.id = p.supplier_id
WHERE ss.units IS NOT NULL
GROUP BY s.country
HAVING ROUND(STDDEV(ss.revenue), 2) >= 79;

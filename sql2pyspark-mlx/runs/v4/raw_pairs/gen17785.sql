SELECT p.category, CASE WHEN p.brand IN ('Initech', 'Stark') THEN 'priority' ELSE 'other' END AS brand_group, COUNT(*) AS store_sale_count
FROM store_sales ss
RIGHT JOIN products p ON ss.product_id = p.id
INNER JOIN stores s ON ss.store_id = s.id
WHERE ss.revenue > 1900
GROUP BY p.category, CASE WHEN p.brand IN ('Initech', 'Stark') THEN 'priority' ELSE 'other' END
HAVING COUNT(*) > 5
ORDER BY brand_group;

SELECT p.supplier_id, p.category, COUNT(*) AS num_inventory
FROM inventory i
LEFT JOIN products p ON i.product_id = p.id
JOIN suppliers s ON s.id = p.supplier_id
WHERE YEAR(p.launch_date) = 2024
GROUP BY p.supplier_id, p.category
HAVING num_inventory > 2
ORDER BY num_inventory DESC;

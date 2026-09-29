SELECT p.brand, SUM(i.quantity) AS total_quantity
FROM inventory i
INNER JOIN products p ON i.product_id = p.id
WHERE p.launch_date BETWEEN '2023-04-01' AND '2024-05-01'
GROUP BY p.brand
HAVING SUM(i.quantity) > 1360;

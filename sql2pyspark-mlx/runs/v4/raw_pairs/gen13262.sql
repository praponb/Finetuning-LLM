SELECT p.category, MAX(i.quantity) - MIN(i.quantity) AS quantity_range FROM inventory i JOIN products p ON p.id = i.product_id GROUP BY p.category ORDER BY quantity_range DESC

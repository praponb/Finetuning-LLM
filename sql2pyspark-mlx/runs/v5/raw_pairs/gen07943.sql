SELECT p.brand, COUNT(i.quantity) AS quantity_count FROM inventory i LEFT JOIN products p ON i.product_id = p.id GROUP BY p.brand ORDER BY brand DESC;

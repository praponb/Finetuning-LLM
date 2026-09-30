SELECT i.store_id, p.product_name, p.price, s.rating, s.country FROM inventory i LEFT JOIN products p ON i.product_id = p.id INNER JOIN suppliers s ON p.supplier_id = s.id ORDER BY rating

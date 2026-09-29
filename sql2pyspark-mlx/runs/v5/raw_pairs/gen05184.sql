SELECT i.quantity, i.product_id, p.supplier_id, p.category, p.launch_date FROM inventory i LEFT OUTER JOIN products p ON i.product_id = p.id WHERE p.price BETWEEN 580 AND 1050

SELECT p.launch_date, p.supplier_id, p.brand, i.product_id, i.quantity FROM products p LEFT OUTER JOIN inventory i ON i.product_id = p.id

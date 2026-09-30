SELECT i.id, i.quantity, p.supplier_id, p.product_name FROM inventory i INNER JOIN products p ON i.product_id = p.id WHERE NOT (p.stock_qty <= 289 AND i.product_id IN (2, 4, 8)) ORDER BY 4, i.id DESC

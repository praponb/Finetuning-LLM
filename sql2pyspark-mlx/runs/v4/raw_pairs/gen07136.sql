SELECT i.product_id, p.stock_qty, s.country FROM inventory i JOIN products p ON i.product_id = p.id FULL OUTER JOIN suppliers s ON p.supplier_id = s.id

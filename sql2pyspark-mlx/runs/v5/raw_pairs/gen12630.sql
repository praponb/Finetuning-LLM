SELECT ss.id, ss.product_id, ss.revenue, ss.store_id, p.brand, p.stock_qty, p.supplier_id FROM store_sales ss INNER JOIN products p ON ss.product_id = p.id WHERE p.stock_qty BETWEEN 171 AND 214;

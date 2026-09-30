SELECT oi.id, oi.product_id, p.stock_qty, p.supplier_id FROM order_items oi LEFT OUTER JOIN products p ON p.id = oi.product_id ORDER BY id DESC

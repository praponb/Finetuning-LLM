SELECT oi.product_id, p.cost, p.category, p.stock_qty FROM order_items oi JOIN products p ON p.id = oi.product_id ORDER BY category ASC;

SELECT cost value, 'product' source FROM products WHERE supplier_id IN (26, 35) UNION ALL SELECT unit_price value, 'order_item' source FROM order_items ORDER BY value DESC;

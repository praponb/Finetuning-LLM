SELECT stock_qty FROM products WHERE brand IN ('Globex', 'Umbrella', 'Acme') EXCEPT SELECT stock_qty FROM products WHERE supplier_id IN (22, 38) ORDER BY stock_qty

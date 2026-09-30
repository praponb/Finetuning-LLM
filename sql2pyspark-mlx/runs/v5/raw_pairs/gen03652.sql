SELECT *, CASE WHEN stock_qty >= 383 THEN 'High' WHEN stock_qty >= 162 THEN 'Medium' ELSE 'Low' END AS stock_qty_tier FROM products WHERE supplier_id = 4

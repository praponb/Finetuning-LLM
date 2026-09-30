SELECT id, stock_qty, category, launch_date, CAST(cost AS INT) cost_int, SUBSTRING(brand, 1, 3) brand_prefix FROM products WHERE stock_qty = 393 AND supplier_id = 28;

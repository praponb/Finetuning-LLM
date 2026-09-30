SELECT id, launch_date, category, COALESCE(product_name, 'N/A') product_name_filled, ROUND(stock_qty, 2) stock_qty_rounded FROM products WHERE category = 'Electronics' AND supplier_id IS NOT NULL

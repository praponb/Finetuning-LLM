SELECT launch_date, 'product' AS origin FROM products WHERE category IN ('Grocery', 'Electronics', 'Sports') UNION ALL SELECT sale_date, 'store_sale' AS origin FROM store_sales

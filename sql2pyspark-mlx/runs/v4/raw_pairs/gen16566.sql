select store_sales.id, store_sales.store_id, products.stock_qty, products.category, products.launch_date from store_sales join products on store_sales.product_id = products.id;

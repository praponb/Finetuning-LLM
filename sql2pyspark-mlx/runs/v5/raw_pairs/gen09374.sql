SELECT id, store_id, product_id, sale_date FROM store_sales QUALIFY ROW_NUMBER() OVER (PARTITION BY product_id, store_id ORDER BY sale_date DESC, id) = 1

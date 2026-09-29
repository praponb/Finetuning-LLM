select store_id, units, sale_date from store_sales where store_id = 4 and (product_id in (1, 5) or product_id = 3);

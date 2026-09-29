SELECT product_id FROM store_sales EXCEPT DISTINCT SELECT product_id FROM store_sales WHERE product_id IN (2, 3, 5, 10);

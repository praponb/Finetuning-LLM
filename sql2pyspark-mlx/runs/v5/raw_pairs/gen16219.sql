SELECT product_id FROM store_sales EXCEPT DISTINCT SELECT product_id FROM store_sales WHERE units <= 25

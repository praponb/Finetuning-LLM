SELECT revenue FROM store_sales WHERE product_id IN (2, 5, 7, 9) EXCEPT SELECT unit_price revenue FROM order_items;

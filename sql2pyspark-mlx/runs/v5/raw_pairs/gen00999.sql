SELECT unit_price AS value FROM order_items EXCEPT SELECT revenue AS value FROM store_sales;

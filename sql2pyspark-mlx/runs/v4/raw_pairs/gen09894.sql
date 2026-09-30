SELECT shipping_fee FROM orders WHERE customer_id IN (7, 40) UNION SELECT revenue FROM store_sales;

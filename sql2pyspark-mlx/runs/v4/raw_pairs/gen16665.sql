SELECT signup_date, 'customer' AS source FROM customers UNION ALL SELECT sale_date, 'store_sale' AS source FROM store_sales WHERE product_id = 5;

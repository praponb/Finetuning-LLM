SELECT sale_date FROM store_sales UNION ALL SELECT order_date FROM orders WHERE shipping_fee >= 19 ORDER BY sale_date DESC

SELECT sale_date, 'store_sale' source FROM store_sales UNION SELECT order_date, 'order' source FROM orders WHERE shipping_fee > 27;

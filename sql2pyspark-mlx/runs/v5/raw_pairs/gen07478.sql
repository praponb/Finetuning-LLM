SELECT 'order' AS origin, order_date FROM orders WHERE channel IS NOT NULL UNION SELECT 'store_sale' AS origin, sale_date FROM store_sales ORDER BY order_date DESC

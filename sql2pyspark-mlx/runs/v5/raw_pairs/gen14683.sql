SELECT 'order_item' AS origin, unit_price FROM order_items UNION SELECT 'store_sale' AS origin, revenue AS unit_price FROM store_sales;

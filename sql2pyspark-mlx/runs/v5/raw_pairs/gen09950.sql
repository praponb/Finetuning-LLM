SELECT 'order_item' AS kind, unit_price AS value FROM order_items WHERE quantity <= 3 UNION ALL SELECT 'store_sale' AS kind, revenue AS value FROM store_sales
